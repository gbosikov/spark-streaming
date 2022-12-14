import time
import signal
from pyspark.sql import functions as F
from typing import Union
from tools_kafka import Kafka
from tools_pyspark_hdfs import Spark_HDFS as HDFS

def stop_all_streams(spark):
    """Останаваливает все стримы"""
    for active_stream in spark.streams.active:
        print(f'Stopping stream: {active_stream}')
        active_stream.stop()

def read_stream_kafka(spark, server, topic_name, schema, maxOffsetsPerTrigger=1):
    """Читает поток из кафки"""
    return spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", server) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger) \
            .load() \
            .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
            .select("value.*", "offset")

def console_stream(stream, processingTime='1 seconds', mode='append'):
    """Пишет в поток writeStream в файловую консоль"""
    return stream.writeStream \
            .format('json') \
            .option('path', 'console') \
            .trigger(processingTime=processingTime) \
            .option("checkpointLocation", 'console/checkpoint') \
            .options(truncate=False) \
            .outputMode(mode) \
            .start()

def console_clear(spark):
    """Очищает файловую консоль"""
    hdfs = HDFS(spark)
    hdfs.rm('console')
    
def console_show(spark):
    """Показывает содержимое файловой консоли"""
    hdfs = HDFS(spark)
    hdfs.cat('console/*', recursive=False, print_df=True)


def console_output(df, freq:int=5):
    """НЕ ИСПОЛЬЗУЕТСЯ. Использовать код где-то ещё или удалить полностью!
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    Воводит содержимое потока spark в консоль. При этом учитывает, 
    что функция может запускаться из ноутбука jupyter. В таком случае
    сначала сохраняет поток в sink и читает из таблицы
    :param df: DataFrame
    :param freq: периодичность вывода. Кол-во секунд.
                 Если скрипт запущен в jupyter, то это кол-во секунд,
                 после которого происходит считывание записанных данных.
    """
    try:
        shell = get_ipython().__class__.__name__
        if shell in ['ZMQInteractiveShell', 'TerminalInteractiveShell']:
            # ZMQInteractiveShell - Jupyter notebook or qtconsole
            # TerminalInteractiveShell - Terminal running IPython
            # stram = sink(df, path='jupyter_console', form='memory')
            # time.sleep(freq)
            # return stram
            # дописать эту функцию!
            return df.writeStream \
                .format("console") \
                .trigger(processingTime=f'{freq} seconds') \
                .options(truncate=False) \
                .start()
            
    except NameError: # вывод в обычную консоль
        return df.writeStream \
            .format("console") \
            .trigger(processingTime=f'{freq} seconds') \
            .options(truncate=False) \
            .start()
        
        
def sink(df, 
         path:str,
         form:str='memory', 
         checkpoint:str='', 
         freq:int=5, 
         timeout:int=0,
         json:bool=False,
         start:bool=True):
    """Сохраняет данные в spark, используя механизмы "sink".
    Может сохранить как в память, так и в файл
    :param df: pyspark.sql.dataframe.DataFrame на который применён load()
    :param form: сохранить в файлах, в памяти или kafka: memory, kafka, parquet
    :param path: если form = memory: название таблицы в памяти
                 если form = kafka: топик
                 если form = parquet: путь к файлу
    :param checkpoint: путь к файлу чекпойнта
    :param freq: периодичность вывода. Кол-во секунд
    :param timeout: кол-во секунд, через сколько остановить поток.
                    Если ноль, то не остановит (доработать эту функцию)
    :param json: работает в режиме sink -> kafka
                    Если True, то передаёт в kafka строки в формате json 
    :param start: запускать ли поток после создания. Если False, то нет. Такой
                    режим используется для установки дополнительных опций,
                    например, вотермарка
    :return: DataFrame
    """
    if 'DataFrame' not in str(type(df)):
        print('Принимает только объекты <pyspark.sql.dataframe.DataFrame>')
        return
        
    if freq <= 1:
        freq = 1
    
    dfr = df
    
    if form == 'memory':
        dfr = df.writeStream \
            .format("memory") \
            .queryName(path) \
            .trigger(processingTime='%s seconds' % freq )
        
    elif form == 'kafka':
        kf = Kafka()
        if path not in kf.ls():
            print(f'Такого топика не существует: {path}')
            print(f'Создаём топик: {path}')
            t = kf.add(name=path, retention=86400*30)
            
        if not checkpoint:
            checkpoint = 'checkpoint_sink_default'
            print(f'Не было передано значение checkpoint.'\
                  f'Указываем принудительно: {checkpoint}')
        
        if json:
            dfr = dfr.selectExpr("CAST(null AS STRING) as key", 
                                 "CAST(to_json(struct(*)) AS STRING) as value")
        else:
            dfr = dfr.selectExpr("CAST(null AS STRING) as key", 
                                 "CAST(struct(*) AS STRING) as value")
        
        dfr = dfr.writeStream \
            .format("kafka") \
            .option("topic", path) \
            .option("kafka.bootstrap.servers", kf.SERVERS)
        
    else:
        dfr = df.writeStream \
            .format("parquet") \
            .trigger(processingTime='%s seconds' % freq ) \
            .option("path", path)
    
    if checkpoint and form == 'memory':
        checkpoint = ''
        print(f'Значение checkpoint передано, но не будет учтено для режима form=memory')
        
    if checkpoint:
        dfr = dfr.option("checkpointLocation", checkpoint)
    
    if start:
        dfr.start()
        
    # try:
    #     signal.alarm(timeout)
    #     исполняемый код, который надо прервать
    #     signal.alarm(0)
    # except Exception as e:
    #     print(e)

    return dfr
