import subprocess
import sys

#NOTE: Вот такого в production ready code просто не может быть, осуществлять системный вызов нельзя.
# Нужно подготавливать файлы requirements.txt

subprocess.check_call([
    sys.executable, '-m', 'pip', 'install', "beautifulsoup4", "SQLAlchemy==1.4.45", "pandas", "numpy",
    "requests", "psycopg2-binary", 'pydantic', 'typing'
])

from datetime import date, timedelta
import json
import time
import requests
import re
import pandas as pd
from uuid import uuid4
from sqlalchemy import create_engine
import prefect
from bs4 import BeautifulSoup
from prefect import task, Flow
from pydantic import BaseModel, ValidationError
from typing import Optional

#NOTE: Объекты, классы, модели, вынести в отдельный файл. Также в файле не присутствует if def main -> смотреть мой файл, может привести к неправильному использованию скрипта.
class DataProcessor:
    def __init__(self, logger):
        self.logger = logger
        self.current_date = date.today()
        self.columns_name = ['id_config', 'cpu_model_1', 'cpu_model_2', 'cpu_gen', 'cpu_count', 'gpu', 'gpu_count',
                             'cores', 'frequency', 'ram', 'ram_type', 'disks', 'datacenter', 'provider', 'price',
                             'date']
        self.columns_all = ['id_config', 'cpu_model_1', 'cpu_model_2', 'cpu_gen', 'cpu_count', 'gpu', 'gpu_count',
                            'cores', 'frequency', 'ram', 'ram_type', 'hdd_size', 'ssd_size', 'nvme_size', 'datacenter',
                            'provider', 'price', 'date']
        self.data_type = {'id_config': object, 'cpu_model_1': str, 'cpu_model_2': str, 'cpu_gen': str, 'cpu_count': int,
                          'gpu': object, 'gpu_count': int, 'cores': int, 'frequency': float, 'ram': int,
                          'ram_type': str, 'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'datacenter': str,
                          'provider': str, 'price': float, 'date': object}
        self.engine = create_engine('postgresql://username:password@localhost/mydatabase')

    # выгрузка данных с веб-сайта
    def extract_data_from_website(self, url):
        self.logger.info(f'Start сonnection to {url}')
        try:
            servers = requests.get(url)
            self.logger.info(f'Successful connection to {url} {servers}')
        except requests.exceptions.HTTPError as err:
            time.sleep(10)
            raise SystemExit(err)
        self.logger.info(f'Extracted data from website{servers.text}')
        return servers

    # вспомогательные фуннкции по обработке данных
    def delete_vendor(self, cpu_name):
        cpu_name = cpu_name.replace(r'AMD', ' ').strip()
        cpu_name = cpu_name.replace(r'Intel', ' ').strip()
        return cpu_name

    def get_cpu_count(self, cpu_name):
        parts = cpu_name.split('x')
        if len(parts) > 1:
            cpu_count = int(parts[0])
            cpu_name = parts[1]
        else:
            cpu_count = 1
        return [cpu_name.strip(), cpu_count]

    def get_data_cpu(self, cpu_name):
        cpu_name = cpu_name.upper()
        x = cpu_name.split('-')
        if len(x) > 1:
            x[1] = x[1].replace(' ', '')
            cpu_name = x[0] + ' ' + x[1]
        cpu_parts = cpu_name.split(' ')
        cpu_model = cpu_parts[0]
        if len(cpu_parts) > 2:
            cpu_gen1 = cpu_parts[1]
            cpu_gen2 = cpu_parts[2]
        else:
            if cpu_parts[1][0].isdigit():
                cpu_gen1 = None
                cpu_gen2 = cpu_parts[1]
            else:
                cpu_gen1 = cpu_parts[1][0]
                cpu_gen2 = cpu_parts[1][1:]
        res = [cpu_model, cpu_gen1, cpu_gen2]
        return res

    # загрузка в базу данных
    def load_to_db(self, provider, data_from_website):
        list_columns = ['cpu_model_1', 'cpu_model_2', 'cpu_gen', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency',
                        'ram', 'ram_type', 'hdd_size', 'ssd_size', 'nvme_size', 'datacenter', 'provider']
        self.logger.info(f'Start loading data to DB')
        # NOTE: SQL Injection warning. Но может и не ворнинг, но я бы использовал ORM. Хотя это тоже ORM от pandas X_X.
        last_config = pd.read_sql_query(f"SELECT * FROM configs WHERE provider = '{provider}'", con=self.engine)
        merge = data_from_website.merge(last_config, on=list_columns, how='left')
        merge = merge.rename(columns={'id_config_x': 'id_config', 'id_config_y': 'last_id'})
        price = merge.loc[~merge['last_id'].isna()]
        price = price[['last_id', 'price', 'date']]
        price = price.rename(columns={'last_id': 'id_config'})
        new_data = merge.loc[merge['last_id'].isna()]
        if ~new_data.empty:
            list_columns.insert(0, 'id_config')
            new_config = new_data[list_columns]
            new_config.to_sql('configs', self.engine, if_exists='append', index=False)
            self.logger.info(f'Added new configs of {provider}: {new_config.shape}')
            new_price = new_data[['id_config', 'price', 'date']]
            price = pd.concat([price, new_price])
        self.logger.info(f'Added prices {provider}: {price.shape}')
        price.to_sql('price', self.engine, if_exists='append', index=False)
        self.logger.info(f'End loading data to DB')


# NOTE: Эталонные структуры json для сайтов не нужны,
# много потраченного времени, нужна только
# финишная pydantic модель которую например можно было отдать мне. :)
class StructureJson_Competitor1(BaseModel):
    body: list

# NOTE: В классах не нижних подчеркиваний, смотри PEP, который прислал в своем образце.
class StructureServer_Competitor1(BaseModel):
    cpu_vendor_short: str
    cpu_cores: int
    cpu_vendor: str
    memory: int
    memory_type: str
    disk_desc: str
    price: str


class Structure_cpu_Competitor2(BaseModel):
    description: str
    number_cores: int
    items: list


class Structure_ram_Competitor2(BaseModel):
    volume: int


class Structure_hard_drive_Competitor2(BaseModel):
    description: str


class Structure_hardware_Competitor2(BaseModel):
    cpu: Structure_cpu_Competitor2
    ram: Structure_ram_Competitor2
    hard_drive: Structure_hard_drive_Competitor2

# NOTE: Одинаковые классы, можно использовать один
class Structure_conditions_Competitor2(BaseModel):
    items: list

class Structure_hard_drive_Competitor2_sale(BaseModel):
    items: list

class Structure_conditions_Competitor2_sale(BaseModel):
    items: list


class Structure_common_Competitor2(BaseModel):
    location: str
    conditions: Structure_conditions_Competitor2


class Structure_Competitor2(BaseModel):
    common: Structure_common_Competitor2
    hardware: Structure_hardware_Competitor2


class Structure_cpu_Competitor2_sale(BaseModel):
    description: str
    number: int
    number_cores: int
    items: list


class Structure_ram_Competitor2_sale(BaseModel):
    volume: int
    type: str


class Structure_graphics_Competitor2_gpu(BaseModel):
    items: str
    number: int


class Structure_hardware_Competitor2_sale(BaseModel):
    cpu: Structure_cpu_Competitor2_sale
    ram: Structure_ram_Competitor2_sale
    hard_drive: Structure_hard_drive_Competitor2_sale
    graphics: Optional[Structure_graphics_Competitor2_gpu] = None


class Structure_common_Competitor2_sale(BaseModel):
    location: str
    conditions: Structure_conditions_Competitor2_sale


class Structure_Competitor2_sale(BaseModel):
    common: Structure_common_Competitor2_sale
    hardware: Structure_hardware_Competitor2_sale


class StructureJson_Competitor2(BaseModel):
    response: list


class Competitor1:
    def __init__(self, logger):
        self.url = 'https://Competitor1.cloud/v1.1/registration/servers'
        # NOTE: Если все в одном файле вообще не понимаю зачем наследование logger и перекидывание его туда сюда,
        # если в нем ничего не настроено. Смотреть мой файл как пример настройки logger'a.
        self.logger = logger
        # NOTE: отсутвие типизации приведет к проблемам, например объект класса logger ниже не имеет типизации. Хотя класс python.
        self.competitor = 'Competitor1'
        self.processor = DataProcessor(logger)

    # NOTE: У функций есть описание их работы. Смотреть мой файл как образец. Один моментик поправил для примера.
    # выгрузка данных с веб-сайта
    @task(max_retries=10, retry_delay=timedelta(seconds=10))
    def extract_data(self):
        """Выгрузка данных c веб-сайта"""
        return self.processor.extract_data_from_website(self.url)

    # валидация структуры json
    def validate_json(self, data, structure):
        try:
            structure.parse_raw(data)
        except ValidationError as e:
            # NOTE: Передала логгер, но ошибки выводишь в print 👍
            self.logger.error(e.errors())
            print(e.errors())
            raise SystemExit(e)

    # извлчение информации об объеме разных типов дисков и о графическом процессоре
    def unpack_disks(self, disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0,
            'gpu': None,
            'gpu_count': 0
        }
        disks = disks.split('+')
        if len(disks) != 1:
            disks[1] = disks[1][1:]
        for disk in disks:
            if ('SSD' not in disk) and ('HDD' not in disk) and ('NVMe' not in disk):
                gpu_data_parts = self.processor.get_cpu_count(disk)
                output['gpu'] = gpu_data_parts[0].upper().replace('GEFORCE', '').strip()
                output['gpu_count'] = gpu_data_parts[1]
            else:
                disk = disk.lower()
                disk = disk.split(' ')
                if disk[3] == 'тб':
                    # NOTE: Same shit bro ✋, TB в GB
                    size = int(disk[0]) * int(disk[2]) * 1000
                else:
                    size = int(disk[0]) * int(disk[2])
                if disk[4] == 'hdd':
                    output[disk[4]] = size
                # NOTE: А в чем смысл ☁️, код делает одно и тоже
                elif disk[4] == 'ssd':
                    output[disk[4]] = size
                else:
                    output[disk[4]] = size
        return output['hdd'], output['ssd'], output['nvme'], output['gpu'], output['gpu_count']

    # извлечение и структурирование выгруженных данных
    @task
    def transform_data(self, data_from_website):
        self.validate_json(data_from_website.text, StructureJson_Competitor1)
        self.logger.info('Start data transforming')
        servers = data_from_website.json()['body']
        transformed_data = pd.DataFrame()
        counter = 0

        for server in servers:
            self.validate_json(json.dumps(server), StructureServer_Competitor1)
            id_config = uuid4() # NOTE: Ну если только ID конфига как понимания, но можно не хранить в отдельной переменной.
            cpu_name = self.processor.delete_vendor(server['cpu_vendor_short'])
            cpu_name_parts = self.processor.get_cpu_count(cpu_name)
            cpu_name = cpu_name_parts[0]
            cpu_count = cpu_name_parts[1]
            cpu_data = self.processor.get_data_cpu(cpu_name)
            cpu_model_1, cpu_model_2, cpu_gen = cpu_data[0], cpu_data[1], cpu_data[2]
            gpu, gpu_count = '', ''
            cores = server['cpu_cores']
            freq = server['cpu_vendor'].split(',')[1].split('-')[0].lower().replace(r' ггц', '')
            ram = int(server['memory']) // 1000
            ram_type = server['memory_type'].split(' ')[0].upper().replace(" ", "")
            disks = server['disk_desc']
            price = server['price']
            date = self.processor.current_date
            datacenter = None
            config_row = [id_config, cpu_model_1, cpu_model_2, cpu_gen, cpu_count,
                          gpu, gpu_count, cores, freq, ram, ram_type, disks,
                          datacenter, self.competitor, price, date]
            # NOTE: Я в pandas почти ничего не понимаю, но выглядит прикольно 💯
            config_row = pd.Series(config_row, index=self.processor.columns_name, name=counter)
            transformed_data = pd.concat([transformed_data, config_row],
                                         axis=1, sort=False)
            counter += 1

        transformed_data = transformed_data.transpose()
        transformed_data['hdd_size'], transformed_data['ssd_size'], transformed_data['nvme_size'], \
        # NOTE: А для чего так сложно, вворачивание в два объекта через zip, почему тут disks, а там gpu и gpu_count 😮‍💨
        transformed_data['gpu'], transformed_data['gpu_count'] = zip(
            *transformed_data['disks'].apply(self.unpack_disks))

        transformed_data = transformed_data[self.processor.columns_all]
        transformed_data = transformed_data.astype(self.processor.data_type)
        # NOTE: Правильна, но нет типизации 🏗️
        self.logger.info(f'Transformed data of {self.competitor}: {transformed_data.shape}')
        return transformed_data

    # загрузка в базу данных
    @task
    def load_to_db(self, transformed_data):
        self.processor.load_to_db(self.competitor, transformed_data)


class Competitor2:
    def __init__(self, logger):
        self.url = [
            'https://api.Competitor2.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=NL&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes',
            'https://api.Competitor2.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=US&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes',
            'https://api.Competitor2.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=RU&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes',
            'https://api.Competitor2.com/v1/inv-api/get-stock-servers?location=&group=!GPU&stock=yes&currency=rub&currencycon=br&servertype=1&pricerate=1&language=ru&name=no',
            'https://api.Competitor2.com/v1/inv-api/get-stock-servers?%20location=&group=gpu&stock=yes&currency=rub&pricerate=1&currencycon=br&servertype=1&name=no&filter=no&language=ru']
        self.logger = logger
        self.competitor = 'hostkey'
        self.processor = DataProcessor(logger)

    # извлечение данных с разных страниц сайтов
    @task(max_retries=10, retry_delay=timedelta(seconds=10))
    def extract_data(self):
        # Выгрузка данных с веб-сайта
        data_from_website = []
        urls = self.url
        for url in urls:
            # NOTE: Тут буква c из другого алфавита
            self.logger.info(f'Start сonnection to {url}')
            try:
                servers = requests.get(url)
                self.logger.info(f'Successful connection to {url} {servers}')
            except requests.exceptions.HTTPError as err:
                time.sleep(10)
                raise SystemExit(err)
            self.logger.info(f'Extracted data from website {servers.text}')
            data_from_website.append(servers)
        return data_from_website

    # валидация структуры json
    def validate_json(self, data, structure):
        try:
            structure.parse_raw(data)
        except ValidationError as e:
            print(e.errors())
            raise SystemExit(e)

    # получение данных об объеме разных типов дисков
    def unpack_disks_Competitor2(self, disk):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        disk = disk.replace(r'sff 10k ', '')
        disk = disk.replace(r'sff 5k ', '')
        disk = disk.replace(r'sas', 'hdd')
        disk = disk.replace(r'u2 ', '')
        disks_parts = disk.split('+')
        for disks in disks_parts:
            disks = disks.strip()
            disks = disks.split(' ')
            size = disks[0].replace('gb', '')
            arg = 1
            if '×' in size:
                parts = size.split('×')
                arg = int(parts[0])
                size = parts[1]
            if 'x' in size:
                parts = size.split('x')
                arg = int(parts[0])
                size = parts[1]
            if 'tb' in size:
                size = size.replace('tb', '')
                size = 1000 * float(size) * arg
            else:
                size = float(size) * arg
            # NOTE: Три условия делают одно и тоже 😄
            if disks[1] == 'hdd':
                output[disks[1]] = size
            if disks[1] == 'ssd':
                output[disks[1]] = size
            if disks[1] == 'nvme':
                output[disks[1]] = size
        return output['hdd'], output['ssd'], output['nvme']

    #NOTE: Смотреть в PEP как писать фунции, там не долнжо быть больших буков. Образец и ссылки в моем коде.
    def unpack_disks_Competitor2_sale_gpu(self, disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        for disk in disks:
            size = float(disk['volume']) * int(disk['count'])
            disk_type = disk['name'].lower()
            disk_type = disk_type.replace(r'sas', 'hdd')
            if 'hdd' in disk_type:
                output['hdd'] = size
            if 'nvme' in disk_type:
                output['nvme'] = size
            elif 'ssd' in disk_type:
                output['ssd'] = size
        return output['hdd'], output['ssd'], output['nvme']

    # создание DataFrame с извлеченными данными для разных страницы сайта

    def sale_gpu_Competitor2(self, data_from_website):
        self.validate_json(data_from_website.text, StructureJson_Competitor2)
        self.logger.info('Start Create df for Competitor2 sale and gpu servers')
        servers = data_from_website.json()['response']
        counter = 0
        data_Competitor2 = pd.DataFrame()
        for server in servers:
            self.validate_json(json.dumps(server), Structure_Competitor2_sale)
            id_config = uuid4()
            cpu = server['hardware']['cpu']['items'][0]
            cpu_name = self.processor.delete_vendor(cpu['name'].replace('xx*', 'XX'))
            cpu_name_parts = cpu_name.split(' ')
            cpu_name = cpu_name_parts[0] + ' ' + cpu_name_parts[1]
            if "GHz" not in cpu_name_parts[2]:
                cpu_name = cpu_name + ' ' + cpu_name_parts[2]
            cpu_data = self.processor.get_data_cpu(cpu_name)
            cpu_model_1, cpu_model_2, cpu_gen = cpu_data[0], cpu_data[1], cpu_data[2]
            cpu_count = server['hardware']['cpu']['number']
            cores = server['hardware']['cpu']['number_cores']
            freq = cpu['ghz']
            ram = server['hardware']['ram']['volume']
            disks = server['hardware']['hard_drive']['items']
            ram_type = server['hardware']['ram']['type'].split(' ')[0].upper().replace(" ", "")
            if ram_type == '':
                ram_type = None
            gpu_data = server['hardware']['graphics']
            if gpu_data is None:
                gpu, gpu_count = None, 0
            else:
                gpu = gpu_data['items'].replace('ATX', '').strip()
                gpu = gpu.replace('Ti', '').strip().upper()
                gpu_count = gpu_data['number']
            date = self.processor.current_date
            datacenter = server['common']['location']
            price = server['common']['conditions']['items'][0]['prices']['current']
            config_row = [id_config, cpu_model_1, cpu_model_2, cpu_gen, cpu_count, gpu, gpu_count, cores, freq, ram,
                          ram_type,
                          disks, datacenter, self.competitor, price, date]
            config_row = pd.Series(config_row, index=self.processor.columns_name, name=counter)
            data_Competitor2 = pd.concat([data_Competitor2, config_row], axis=1, sort=False)
            counter += 1

        data_Competitor2 = data_Competitor2.transpose()
        # NOTE: Вот тут диски в диски пошли 🚶
        data_Competitor2['hdd_size'], data_Competitor2['ssd_size'], data_Competitor2['nvme_size'] = zip(
            *data_Competitor2['disks'].apply(self.unpack_disks_Competitor2_sale_gpu))
        data_Competitor2 = data_Competitor2[self.processor.columns_all]
        return (data_Competitor2)

    def create_df_Competitor2(self, data_from_website):
        self.validate_json(data_from_website.text, StructureJson_Competitor2)
        self.logger.info('Start Create df for Competitor2 base servers')
        Competitor2_servers = data_from_website.json()['response']
        data_Competitor2y = pd.DataFrame() # NOTE: Неиспользуемая переменная. Используй проверку и форматирование кода black, isort, pylance. 🐬
        counter = 0

        for server in Competitor2_servers:
            self.validate_json(json.dumps(server), Structure_Competitor2)
            # NOTE: Какой-то принт зачесался 💇‍♂️
            print(server)
            id_config = uuid4()
            cpu_name = self.processor.delete_vendor(server['hardware']['cpu']['description'].replace('xx*', 'XX'))
            cpu_name_parts = self.processor.get_cpu_count(cpu_name)
            cpu_name, cpu_count = cpu_name_parts[0], cpu_name_parts[1]
            if re.match(r'E\d*-', cpu_name):
                cpu_name = 'Xeon ' + cpu_name
            if re.match(r'i\d*', cpu_name):
                cpu_name = 'Core ' + cpu_name
            cpu_data = self.processor.get_data_cpu(cpu_name)
            cpu_model_1, cpu_model_2, cpu_gen = cpu_data[0], cpu_data[1], cpu_data[2]
            cores = server['hardware']['cpu']['number_cores']
            freq = server['hardware']['cpu']['items'][0]['ghz']
            ram = server['hardware']['ram']['volume']
            disks = server['hardware']['hard_drive']['description'].lower()
            gpu = ram_type = None
            gpu_count = 0
            date = self.processor.current_date
            datacenter = server['common']['location']
            price = server['common']['conditions']['items'][0]['prices']['current']
            config_row = [id_config, cpu_model_1, cpu_model_2, cpu_gen, cpu_count, gpu, gpu_count, cores, freq, ram,
                          ram_type,
                          disks, datacenter, self.competitor, price, date]
            config_row = pd.Series(config_row, index=self.processor.columns_name, name=counter)
            data_Competitor2 = pd.concat([data_Competitor2, config_row], axis=1, sort=False)
            counter += 1

        data_Competitor2 = data_Competitor2.transpose()
        data_Competitor2['hdd_size'], data_Competitor2['ssd_size'], data_Competitor2['nvme_size'] = zip(
            *data_Competitor2['disks'].apply(self.unpack_disks_Competitor2))
        data_Competitor2 = data_Competitor2[self.processor.columns_all]
        return data_Competitor2

    # преобразование данных
    @task
    def transform_data(self, data_from_website):
        self.logger.info('Start Competitor2 data transforming')
        d_NL = self.create_df_Competitor2(data_from_website[0])
        d_USA = self.create_df_Competitor2(data_from_website[1])
        d_R = self.create_df_Competitor2(data_from_website[2])
        d_sale = self.sale_gpu_Competitor2(data_from_website[3])
        d_gpu = self.sale_gpu_Competitor2(data_from_website[4])
        res_Competitor2 = pd.concat([d_NL, d_USA, d_R, d_sale, d_gpu])
        res_Competitor2 = res_Competitor2.astype(self.processor.data_type)
        self.logger.info(f'Transformed data of {self.competitor}: {res_Competitor2.shape}')
        return (res_Competitor2)

    # закгрузка в базу данных
    @task
    def load_to_db(self, transformed_data):
        self.processor.load_to_db(self.competitor, transformed_data)


class Competitor3:
    def __init__(self, logger):
        self.url = 'https://www.Competitor3/dedicated_servers'
        self.logger = logger
        self.competitor = 'Competitor3'
        self.processor = DataProcessor(logger)

    # выгрузка данных с веб-сайта
    @task(max_retries=10, retry_delay=timedelta(seconds=10))
    def extract_data(self):
        return self.processor.extract_data_from_website(self.url)

    # извлечение данных об объеме каждого типов дисков
    def unpack_disks(self, disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        disks = re.findall(r'(\d+) x (\d+\.\d+|\d+) (тб|гб) (ssd|hdd|nvme)', disks)
        for disk in disks:
            if disk[2] == "тб":
                size = 1000 * int(disk[0]) * float(disk[1])
                output[disk[3]] = output[disk[3]] + size
            else:
                size = int(disk[0]) * float(disk[1])
                output[disk[3]] = output[disk[3]] + size
        return output['hdd'], output['ssd'], output['nvme']

    # Функция для извлечения и структурирования данных
    @task
    def transform_data(self, data_from_website):
        self.logger.info('Start data transforming')
        # Создаем объект Beautiful Soup для парсинга HTML-кода
        soup = BeautifulSoup(data_from_website.content, "html.parser")
        # Находим все элементы <div> с классом 'b-dedicated-servers-list-item__con
        # tent' с помощью метода findAll()
        servers = soup.findAll('div', class_='b-dedicated-servers-list-item__content')
        # создание DataFrame для структурированных данных
        transformed_data = pd.DataFrame()
        counter = 0

        # Итерация по списку cерверов
        for server in servers:
            id_config = uuid4()
            # Находим первый элементы <span> с классом 'b-dedicated-servers-list-item__title' с помощью метода find()







            #NOTE: Такое количества кода почти нечитаемое 😸, побольше функций.




            # и получаем его содержимое выполнив get_text().
            cpu_name = \
                server.find('span', class_='b-dedicated-servers-list-item__title').get_text().strip().split('сервера')[
                    1]
            cpu_name = self.processor.delete_vendor(cpu_name)
            cpu_name_parts = self.processor.get_cpu_count(cpu_name)
            cpu_name = cpu_name_parts[0]
            cpu_count = cpu_name_parts[1]
            cpu_data = self.processor.get_data_cpu(cpu_name)
            cpu_model_1, cpu_model_2, cpu_gen = cpu_data[0], cpu_data[1], cpu_data[2]
            cores = server.find('span', class_='b-dedicated-servers-list-item__subtitle').get_text().strip()
            ram_data = server.find('div', class_='b-dedicated-servers-list-item__ram').get_text().strip().split('ГБ')
            frequency = ''
            ram, ram_type = ram_data[0], ram_data[1]
            disks = server.find('div', class_='b-dedicated-servers-list-item__hdd').decode_contents()
            if '<br/>' in str(disks):
                disks = str(disks)
                disks = re.sub('<br/>', ' | ', disks)
                disks = re.sub('<.*?>', '', disks)
                disks = disks.strip()
            else:
                disks = disks.strip()
            price = server.find('div', class_='b-dedicated-servers-list-item__current-price').get_text().strip()
            datacenter = server.find('span', class_='b-dedicated-servers-list-item__address').get_text().strip()
            date = self.processor.current_date
            gpu = None
            gpu_count = 0
            config_row = [id_config, cpu_model_1, cpu_model_2, cpu_gen, cpu_count, gpu, gpu_count, cores, frequency,
                          ram, ram_type, disks,
                          datacenter, self.competitor, price, date]
            config_row = pd.Series(config_row, index=self.processor.columns_name, name=counter)
            transformed_data = pd.concat([transformed_data, config_row], axis=1, sort=False)
            counter += 1

        transformed_data = transformed_data.transpose()
        transformed_data['frequency'] = transformed_data['cores'].str.extract(r'(\d\.\d+)').astype(float)
        transformed_data['cores'] = transformed_data['cores'].str.extract(r'(\d+) ')
        transformed_data['ram'] = transformed_data['ram'].str.extract(r'(\d+) ')
        transformed_data['price'] = transformed_data['price'].str.extract(r'(\d+\s\d+)')
        transformed_data['disks'] = transformed_data['disks'].str.replace(r'(sas|sata)', 'hdd')
        transformed_data['hdd_size'], transformed_data['ssd_size'], transformed_data['nvme_size'] = zip(
            *transformed_data['disks'].apply(self.unpack_disks))
        transformed_data = transformed_data[[self.processor.columns_all]]
        transformed_data = transformed_data.astype(self.processor.data_type)
        self.logger.info(f'Transformed data of {self.competitor}: {transformed_data.shape}')

        return transformed_data

    # загрузка в базу данных
    # NOTE: upload_to_db, правильнее.
    @task
    def load_to_db(self, transformed_data):
        self.processor.load_to_db(self.competitor, transformed_data)


with Flow('load_competitors_data_script') as flow:
    logger = prefect.context.get("logger")
    # создание экземпляров класса для работы с данными конкурентов
    parsers = [Competitor1(logger), Competitor2(logger), Competitor3(logger)]
    for parser in parsers:
        # выгрузка данных с веб-сайтов
        data_from_website = parser.extract_data(parser)
        # обработка и структурирование данных
        transformed_data = parser.transform_data(parser, data_from_website)
        # сохранение в базу данных
        parser.load_to_db(parser, transformed_data)
