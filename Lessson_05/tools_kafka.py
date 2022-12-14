from typing import Union
import subprocess

class Kafka():
    HOMEDIR = '/home/hsk/kafka/bin'  # путь к папке с исполняемыми файлами Кафки: kafka-topics.sh...
    SERVERS = 'localhost:9092'  # адреса серверов с портами через запятую
    
    def __init__(self, servers:str='', homedir:str=''):
        if servers:
            self.SERVERS = servers
        if homedir:
            self.HOMEDIR = homedir

    def _decode_bash_output(self, output:bytes) -> list:
        """Преобразует вывод bash команд в список
        :param name: название топика
        :return: список со строками, которые вывелись в bash
        """
        return list(filter(len, output.decode('utf-8').split('\n')))

    def add(self, name:Union[str, list], retention:float=86400*30) -> bool:
        """Создаёт топик в kafka
        :param name: название топика
        :param retention: время жизни информации в топике, секунды
        :return: Ture (если успешно) и False (если проблема)
        """
        retention *= 1000
        retention = int(retention)
        
        names = name
        if isinstance(name, str):
            names = [name]
        
        result = True
        for name in names:
            if name in self.ls():
                print(f'Топик уже существует: {name}')
                continue

            bashCommand = f'{self.HOMEDIR}/kafka-topics.sh --bootstrap-server {self.SERVERS} ' +\
                          f'--create --topic {name} --replication-factor 1 --partitions 1 ' +\
                          f'--config retention.ms={retention}'
            
            process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            output = self._decode_bash_output(output)

            if name not in self.ls():
                if output:
                    print(output)
                print(f'Не удалось создать топик: {name}')
                result = False
            else:
                print(f'Топик успешно создан: {name}')
            
            if error:
                if output:
                    print(output)
                result = False

        return result

    def ls(self) -> list:
        """Список топиков в kafka
        :return: список
        """
        bashCommand = f'{self.HOMEDIR}/kafka-topics.sh --bootstrap-server {self.SERVERS} --list'

        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        output = self._decode_bash_output(output)
        if error or not len(output):
            return []

        return output

    def rm(self, name:Union[str, list]) -> bool:
        """Удаляет топик в kafka
        :param name: название топика или список из названий
        :return: Ture - если успешно созданы все топики
                 False - если была ошибка хотя бы с одним
                        (остуствие топика не считается ошибкой)
        """
        names = name
        if isinstance(name, str):
            names = [name]
        
        result = True
        for name in names:
            if name not in self.ls():
                print(f'Такого топика не существует: {name}')
                continue

            bashCommand = f'{self.HOMEDIR}/kafka-topics.sh --bootstrap-server {self.SERVERS} ' +\
                          f'--delete --topic {name}'
            
            process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            output = self._decode_bash_output(output)

            if name in self.ls():
                if output:
                    print(output)
                print(f'Не удалось удалить топик: {name}')
                result = False
            else:
                print(f'Топик успешно удален: {name}')

            if error:
                if output:
                    print(output)
                result = False
        
        return result
    
    def get(self, name:Union[str, list], from_beginning:bool=True, timeout:float=1) -> None:
        """Читает все текущие сообщения в топике kafka. После окончания не продолжает следить за потоком
        :param name: название топика или список из названий
        :param from_beginning: начать выдачу с самого начала истории
        :param timeout: кол-во секунд, когда оборвать работу и выдать результат
        """
        timeout = int(timeout*1000)
        if name not in self.ls():
            print(f'Такого топика не существует: {name}')
            return None

        bashCommand = f'{self.HOMEDIR}/kafka-console-consumer.sh --bootstrap-server {self.SERVERS} ' +\
                      f'--topic {name} --timeout-ms {timeout}  2>/dev/null'

        if from_beginning:
            bashCommand += ' --from-beginning'
        
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        output = self._decode_bash_output(output)
        if output:
            for ln in output:
                print(ln)
        else:
            print(output)
