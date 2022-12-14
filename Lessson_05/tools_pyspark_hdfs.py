import json
from typing import Union
from pyspark.sql import Row

class Spark_HDFS():
    SPARK = None # spark context
    SC = None # spark context
    FS = None # hdfs FileSystem in spark
    Path = None # функция получения java идентификаторов файлов в hdfs
    
    def __init__(self, spark):
        self.SPARK = spark
        self.SC = spark.sparkContext
        self.Path = self.SC._jvm.org.apache.hadoop.fs.Path
        self.FS = (self.SC._jvm.org
                  .apache.hadoop
                  .fs.FileSystem
                  .get(self.SC._jsc.hadoopConfiguration()) )
    
    def sizeof_fmt(self, num, suffix="B"):
        """Размер файлов в человеко читаемом вид
        :param num: кол-во байт
        е"""
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if num < 1024.0:
                return f"{num:3.1f} {unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f} Yi{suffix}"
    

    def files(self, path:str, recursive:bool=False) -> list:
        """получает список файлов или директорий по указанному пути
        :param path: путь к директории внутри HDFS
        :param recursive: рекурсивно, Выводит только файлы, но не папки!!!
        """
        files = []
        
        if recursive:
            status = self.FS.listFiles(self.Path(path), recursive)
                
            while status.hasNext():
                pth = status.next().getPath().toString()
                files.append(pth)
        else:
            status = self.FS.listStatus(self.Path(path))

            for file in status:
                pth = file.getPath().toString()
                files.append(pth)
            
        return files
        
        
    def ls(self, 
           path:str, 
           recursive:bool=False, 
           return_paths:bool=False) -> Union[list, None]:
        """Показывает список файлов в директории HDFS по заданному пути
        :param path: путь к директории внутри HDFS
        :param recursive: рекурсивно
        :param return_list: возвратить список путей к файлам
        """
        files = self.files(path=path, recursive=recursive)
            
        if files:
            print(f'Список файлов в директории {path}:')
            for f in files:
                print(f)
        else:
            print(f'Нет файлов в директории {path}')
        
        if return_paths:
            return files
    
    def du(self, 
           path:str, 
           recursive:bool=False, 
           return_paths:bool=False, 
           human_readable:bool=True) -> Union[list, None]:
        """Показывает список файлов и их размеры в директории HDFS по заданному пути
        :param path: путь к директории внутри HDFS
        :param recursive: рекурсивно
        :param return_paths: возвратить список путей к файлам и их размеры
        :param human_readable: размер файлов преобразуется в человеко понытные значения
        """
        files = self.files(path=path, recursive=recursive)
        
        res = []
        for pth_str in files:
            file_obj = self.Path(pth_str)
            tp = 'dir' if self.FS.isDirectory(file_obj) else 'file'
            size = self.FS.getContentSummary(file_obj).getSpaceConsumed()
            if human_readable:
                size = self.sizeof_fmt(size)
            res.append({
                'size': size, 
                'type': tp, 
                'path': pth_str, 
            })
            
        if res:
            print(f"Тип\tразмер\t\tпуть")
            for f in res:
                print(f"{f['type']}\t{f['size']}\t\t{f['path']}")
        else:
            print(f'Нет файлов в директории {path}')
        
        if return_paths:
            return res
    
    def read(self, path:str) -> list:
        """Читает содержимое файла в список по строкам
        :param path: путь к файлу внутри HDFS
        :return: список строк
        """
        out = self.FS.openFile(self.Path(path)) \
            .build() \
            .get() #  FSDataInputStreamBuilder
        
        lines = []
        while True:
            ln = out.readLine()
            if ln:
                lines.append(ln)
            else:
                break
                
        return lines
                
    def cat(self, 
            path:str, 
            verbose:bool=False, 
            recursive:bool=False,
            print_df:bool=False) -> None:
        """Читает содержимое файла в консоль/
        Если в конце пути указать *, то будут прочитаны все файлы
        :param path: путь к директории внутри HDFS
        :param verbose: выводить названия файлов.
        :param recursive: читать рекурсивно. Используется при /* в конце пути.
        :param print_df: печатает датафрейм. Используется при /* в конце пути.
                            Для этого в файлах дожны находиться 
                            json строки с одинаковыми столбцами
        """
        if path.endswith('/*'):
            path = path[:-1]
            if not self.FS.exists(self.Path(path)):
                if verbose:
                    print(f'Нет такой папки: {path}')
                return
            
            folder_files = self.files(path=path, recursive=recursive)
            json_lines = []
            for pth_str in folder_files:
                
                file_obj = self.Path(pth_str)
                if self.FS.isDirectory(file_obj):
                    continue
                
                if verbose:
                    print(f'Содержимое файла {path}:')
                    
                
                lines = self.read(pth_str)
                
                if print_df:
                    for ln in lines:
                        try:
                            json_lines.append(json.loads(ln))
                        except Exception as e:
                            if verbose:
                                print(f'Строка в json файле имет неверный формат')
                else:
                    for ln in lines:
                        print(ln)
            
            if print_df:
                if not json_lines:
                    json_lines=[{'Нет данных для построения DataFrame': 
                                f'Было обработано файлов: {len(json_lines)}'}]
                    if not len(folder_files):
                        json_lines=[{'Нет данных для построения DataFrame': 
                                        'Не создано ещё ни одного файла'}]
                self.SPARK.createDataFrame(Row(**x) for x in json_lines).show(truncate=False)
                    
            return
        
        file_obj = self.Path(path)
        if self.FS.isDirectory(file_obj):
            print(f'Читать можно только содержимое файлов, но не директорий: {path}')
            return
        
        if verbose:
            print(f'Содержимое файла {path}:')
            
        lines = self.read(path)
        for ln in lines:
            print(ln)

    def rm(self, path:str, recursive:bool=True) -> bool:
        """Удаляет файл в HDFS по заданному пути
        :param path: путь к файлу внутри HDFS
        :param recursive: удалить рекурсивно
        :return: Ture (если успешно) и False (если проблема)
        """
        res = self.FS.delete(self.Path(path), True)
        msg = f'Файл успешно удалён: {path}' if res else f'Не удалось удалить файл: {path}'
        print(msg)
        return res

    def put(self, local_file:str, hdfs_file:str, delSrc:bool=False, overwrite:bool=True) -> bool:
        """Копирует локальный файл в HDFS
        :param local_file: путь к локальному файлу 
        :param hdfs_file: путь к файлу внутри HDFS
        :param delSrc: удалить файл-источник после успешной передачи
        :param overwrite: перезаписать файл, если он уже существует
        :return: Ture (если успешно) и False (если проблема)
        """ 
        res = True
        try:
            self.FS.copyFromLocalFile(delSrc, 
                                 overwrite, 
                                 self.Path(local_file), 
                                 self.Path(hdfs_file)
                                )
        except Exception as e:
            print(e)
            res = False

        msg = f'Файл успешно скопирован: {local_file}' if res else f'Ну удалось скопировать файл: {local_file}'
        print(msg)
        return res 
