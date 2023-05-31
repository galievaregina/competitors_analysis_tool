import subprocess
import sys

subprocess.check_call([
    sys.executable, '-m', 'pip', 'install', "SQLAlchemy==1.4.45", "pandas", "numpy", "psycopg2-binary", 'pydantic'
])

from datetime import date, timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import task, Flow
import prefect


class Company:
    def __init__(self, logger):
        self.columns_name = ['id_config', 'cpu_model_1', 'cpu_model_2', 'cpu_gen', 'cpu_count', 'gpu', 'gpu_count',
                             'cores',
                             'frequency',
                             'ram', 'ram_type', 'disks', 'price']
        self.columns_all = ['id_config', 'cpu_model_1', 'cpu_model_2', 'cpu_gen', 'cpu_count', 'gpu', 'gpu_count',
                            'cores',
                            'frequency', 'ram',
                            'ram_type', 'hdd_size', 'ssd_size', 'nvme_size', 'price']
        self.data_type = {'id_config': str, 'cpu_model_1': str, 'cpu_model_2': str, 'cpu_gen': str, 'cpu_count': int,
                          'gpu': object,
                          'gpu_count': int, 'cores': int, 'frequency': float, 'ram': int, 'ram_type': str,
                          'hdd_size': int,
                          'ssd_size': int,
                          'nvme_size': int, 'price': float}
        self.engine_company_db = create_engine('postgresql://username:password@localhost/mydatabase')
        self.logger = logger

    @task
    def get_data_from_company_db(self):
        company_data = pd.read_sql_query('SELECT hss.cpu_name, hss.cpu_base_freq, hss.cpu_count,'
                                         ' hss.cores_per_cpu, hss.gpu_name,hss.gpu_count, hss.disk, '
                                         'hss.ram, hs.multi_lang_name as id_config, hs.price_collection as price '
                                         'FROM havik_service_server hss '
                                         'LEFT JOIN havik_service hs '
                                         'ON hs.uuid = hss.uuid WHERE hs.state_id in (1,2,4)',
                                         con=self.engine_company_db)
        company_data = company_data.to_dict("records")
        self.logger.info(f'Data from internal db: {company_data}')
        return company_data

    def unpack_disks_company(self, disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        for disk in disks:
            size = float(disk['size']) * int(disk['count'])
            disk_type = disk['type'].lower()
            if 'hdd' in disk_type:
                output['hdd'] = size
            if 'nvme' in disk_type:
                output['nvme'] = size
            elif 'ssd' in disk_type:
                output['ssd'] = size
        return output['hdd'], output['ssd'], output['nvme']

    def delete_vendor(self, cpu_name):
        cpu_name = cpu_name.replace(r'AMD', ' ').strip()
        cpu_name = cpu_name.replace(r'Intel', ' ').strip()
        return cpu_name

    def get_data_cpu(self, cpu_name):
        cpu_name = cpu_name.upper()
        x = cpu_name.split('-')
        if len(x) > 1:
            x[1] = x[1].replace(' ', '')
            cpu_name = x[0] + ' ' + x[1]
        cpu_parts = cpu_name.split(' ')
        if len(cpu_parts) > 2:
            cpu_model = cpu_parts[0]
            cpu_gen1 = cpu_parts[1]
            cpu_gen2 = cpu_parts[2]
        else:
            cpu_model = cpu_parts[0]
            cpu_gen1 = None
            cpu_gen2 = cpu_parts[1]
        res = [cpu_model, cpu_gen1, cpu_gen2]
        return res

    @task
    def transform_company_data(self, company_data):
        counter = 0
        data_company = pd.DataFrame()
        for server in company_data:
            id_config = server['id_config']['en']
            if 'S LAB.' not in id_config:
                cpu_name = self.delete_vendor(server['cpu_name'])
                cpu_data = self.get_data_cpu(cpu_name)
                cpu_model_1, cpu_model_2, cpu_gen = cpu_data[0], cpu_data[1], cpu_data[2]
                cpu_count = server['cpu_count']
                freq = server['cpu_base_freq']
                cores = int(server['cores_per_cpu']) * cpu_count
                gpu = server['gpu_name']
                if len(gpu) > 1:
                    gpu_data = gpu.split(' ')
                    if gpu_data[1] == 'Tesla':
                        gpu = gpu_data[1] + ' ' + gpu_data[2]
                    else:
                        gpu = gpu_data[0] + ' ' + gpu_data[1]
                gpu = gpu.strip().upper()
                gpu_count = server['gpu_count']
                ram = server['ram'][0]['size'] * server['ram'][0]['count']
                ram_type = server['ram'][0]['type'].upper()
                disks = server['disk']
                price = server['price']['RUB']['month']
                config_row = [id_config, cpu_model_1, cpu_model_2, cpu_gen, cpu_count, gpu, gpu_count, cores, freq, ram,
                              ram_type,
                              disks, price]
                config_row = pd.Series(config_row, index=self.columns_name, name=counter)
                data_company = pd.concat([data_company, config_row], axis=1, sort=False)
                counter += 1
        data_company = data_company.transpose()
        data_company['hdd_size'], data_company['ssd_size'], data_company['nvme_size'] = zip(
            *data_company['disks'].apply(self.unpack_disks_company))
        data_company['gpu'] = data_company['gpu'].str.replace("GPU A2000", "RTX A2000")
        data_company = data_company[self.columns_all]
        data_company = data_company.astype(self.data_type)
        self.logger.info(f'Transformed company data: {data_company}')
        return data_company


class Comparator:
    def __init__(self, competitor, logger):
        self.competitor = competitor
        self.engine_db_competitor_configs = create_engine('postgresql://username:password@localhost/mydatabase')
        self.logger = logger
        self.end_date = date.today()
        self.start_date = self.end_date - timedelta(days=30)
        self.competitor_configs = pd.read_sql_query(f"SELECT * "
                                                    f"FROM configs "
                                                    f"RIGHT JOIN (SELECT id_config as p_id, price,date "
                                                                f"FROM price "
                                                                f"WHERE date "
                                                                f"BETWEEN '{self.start_date}' AND '{self.end_date}') as p "
                                                    f"ON configs.id_config = p.p_id "
                                                    f"WHERE provider = '{self.competitor}';",
                                                    con=self.engine_db_competitor_configs)

    def compare_disks(self, config, similar):
        if config['nvme_size'] != 0:
            similar = similar.loc[similar['nvme_size'] != 0]
        else:
            similar = similar.loc[similar['nvme_size'] == 0]
        if config['ssd_size'] != 0:
            similar = similar.loc[similar['ssd_size'] != 0]
        else:
            similar = similar.loc[similar['ssd_size'] == 0]
        if config['hdd_size'] != 0:
            similar = similar.loc[similar['hdd_size'] != 0]
        else:
            similar = similar.loc[similar['hdd_size'] == 0]
        similar['diff_hdd'] = abs(similar['hdd_size'] - config['hdd_size'])
        similar['diff_ssd'] = abs(similar['ssd_size'] - config['ssd_size'])
        similar['diff_nvme'] = abs(similar['nvme_size'] - config['nvme_size'])
        similar = similar.loc[
            (similar['diff_hdd'] == similar['diff_hdd'].min()) & (similar['diff_ssd'] == similar['diff_ssd'].min()) & (
                    similar['nvme_size'] == similar['nvme_size'].min())]
        del similar['diff_hdd'], similar['diff_ssd'], similar['diff_nvme']
        return similar

    def compare_gen(self, config, similar):
        if config['cpu_model_2'] == 'SILVER' or config['cpu_model_2'] == 'GOLD':
            if config['cpu_gen'][1:2] == '1' or config['cpu_gen'][1:2] == '2':
                return similar.loc[(similar['cpu_gen'].str[1:2] == '1') | (similar['cpu_gen'].str[1:2] == '2')]
            elif config['cpu_gen'][1:2] == '3' or config['cpu_gen'][1:2] == '4':
                return similar.loc[(similar['cpu_gen'].str[1:2] == '3') | (similar['cpu_gen'].str[1:2] == '4')]
        if config['cpu_model_2'] == 'E3' or config['cpu_model_2'] == 'E5' or config['cpu_model_2'] == 'E' or config[
            'cpu_model_2'] == 'W':
            return similar.loc[similar['cpu_gen'].str[0:2] == config['cpu_gen'][0:2]]
        if config['cpu_model_1'] == 'RYZEN' or config['cpu_model_1'] == 'EPYC':
            if config['cpu_model_1'][-1].isdigit():
                return similar.loc[similar['cpu_gen'].str[-1] == config['cpu_gen'][-1]]
            else:
                return similar.loc[similar['cpu_gen'].str[-2] == config['cpu_gen'][-2]]
        else:
            return similar

    def split_by_gpu(self, df):
        df_not_gpu = df.loc[df['gpu_count'] == 0]
        df_gpu = df.loc[df['gpu_count'] != 0]
        return [df_not_gpu, df_gpu]

    def compare_configs(self, competitor_configs, company_config):
        if company_config['cpu_model_2'] == 'SILVER' or company_config['cpu_model_2'] == 'GOLD':
            similar = competitor_configs.loc[
                (competitor_configs['cpu_model_2'] == 'SILVER') | (competitor_configs['cpu_model_2'] == 'GOLD')]
        else:
            similar = competitor_configs.loc[competitor_configs['cpu_model_2'] == company_config['cpu_model_2']]
        if similar.loc[similar['ram_type'] == company_config['ram_type']].empty:
            similar = similar.loc[similar['ram_type'] == 'None']
        else:
            similar = similar.loc[similar['ram_type'] == company_config['ram_type']]
        similar = similar.loc[(similar['cpu_model_1'] == company_config['cpu_model_1'])
                              & (similar['cpu_count'] == company_config['cpu_count'])
                              & (similar['cores'] == company_config['cores'])]
        total_match = similar.loc[(similar['cpu_gen'] == company_config['cpu_gen'])
                                  & (similar['ram'] == company_config['ram']) & (
                                          similar['hdd_size'] == company_config['hdd_size'])
                                  & (similar['ssd_size'] == company_config['ssd_size']) & (
                                          similar['nvme_size'] == company_config['nvme_size'])
                                  & (similar['frequency'] == company_config['frequency'])]
        if total_match.empty:
            # cpu_gen
            similar = self.compare_gen(company_config, similar)
            # ram_size
            if company_config['ram'] < 128:
                similar = similar.loc[
                    (similar['ram'] >= company_config['ram'] / 2) & (similar['ram'] <= company_config['ram'] * 2)]
            else:
                similar = similar.loc[(similar['ram'] >= (3 * company_config['ram']) / 4) & (
                        similar['ram'] <= (5 * company_config['ram']) / 4)]
            similar['diff_ram_size'] = abs(similar['ram'] - company_config['ram'])
            min_diff = similar['diff_ram_size'].min()
            similar = similar.loc[similar['diff_ram_size'] == min_diff]
            del similar['diff_ram_size']
            # disks type and near size
            similar = self.compare_disks(company_config, similar)
            if not similar.loc[similar['frequency'] == company_config['frequency']].empty:
                similar = similar.loc[similar['frequency'] == company_config['frequency']]
            similar['total_match'] = 0
        else:
            similar = total_match
            similar['total_match'] = 1
        return similar

    def compare_gpu_configs(self, competitor_config, company_config):
        gpu_analogs = {'RTX A2000': ['GTX 1660', 'RTX 2050', 'RTX 2060', 'RTX 4050'],
                       'GTX 1080': ['GTX 1070', 'RTX 2070', 'RTX 3050', 'RTX 3060', 'RTX 3070'],
                       'RTX 2080': ['RTX 3080'],
                       'TESLA T4': ['A2', 'RTX A4000', 'RTX 4080'],
                       'GPU A2': ['TESLA T4', 'RTX A4000', 'RTX 4080'],
                       'RTX A4000': ['TESLA T4', 'A2', 'RTX 4080'],
                       'RTX A5000': ['A10', 'A30', 'RTX 3090' 'RTX 4090'],
                       'TESLA V100': ['TESLA V100S'],
                       'TESLA A100': ['TESLA V100', 'TESLA V100S', 'RTX A6000']}
        gpu_company = company_config['gpu']
        if not competitor_config.loc[competitor_config['gpu'] == gpu_company].empty:
            similar = competitor_config.loc[competitor_config['gpu'] == gpu_company]
            if not similar.loc[similar['gpu_count'] == company_config['gpu_count']].empty:
                similar = similar.loc[similar['gpu_count'] == company_config['gpu_count']]
                similar['total_match'] = 1
            else:
                similar['total_match'] = 0
        elif gpu_company in gpu_analogs.keys():
            analogs = gpu_analogs[gpu_company]
            similar = competitor_config.loc[competitor_config['gpu'].isin(analogs)]
            if not similar.loc[similar['gpu_count'] == company_config['gpu_count']].empty:
                similar = similar.loc[similar['gpu_count'] == company_config['gpu_count']]
            similar['total_match'] = 0
        return similar

    @task
    def find_similar_configs(self, company_configs):
        competitor_configs = self.competitor_configs.sort_values('date').drop_duplicates(subset=['id_config'],
                                                                                         keep='last')
        del competitor_configs['p_id'], competitor_configs['date']
        matching = pd.DataFrame()
        counter = 0
        company_not_gpu, company_gpu = self.split_by_gpu(company_configs)
        competitor_configs_not_gpu, competitor_configs_gpu = self.split_by_gpu(competitor_configs)
        # WITHOU GPU
        for index, company_config in company_not_gpu.iterrows():
            similar = self.compare_configs(competitor_configs_not_gpu, company_config)
            for i, competitor_config in similar.iterrows():
                config_row = pd.Series(
                    [company_config['id_config'], competitor_config['id_config'], competitor_config['total_match']],
                    index=['company_conf_id', 'competitor_conf_id', 'total_match'], name=counter)
                matching = pd.concat([matching, config_row], axis=1, sort=False)
                counter += 1
        for index, company_config in company_gpu.iterrows():
            similar = self.compare_gpu_configs(competitor_configs_gpu, company_config)
            for i, competitor_config in similar.iterrows():
                config_row = pd.Series(
                    [company_config['id_config'], competitor_config['id_config'], competitor_config['total_match']],
                    index=['company_conf_id', 'competitor_conf_id', 'total_match'], name=counter)
                matching = pd.concat([matching, config_row], axis=1, sort=False)
                counter += 1
        matching = matching.transpose()
        self.logger.info(f'{self.competitor} configs matching with company: {matching}')
        return matching

    @task
    def load_to_db(self, new_matching):
        config_matching_db = pd.read_sql_query(f"SELECT * FROM competitors_configs_matching;",
                                               con=self.engine_db_competitor_configs)
        existing_matching = new_matching.merge(config_matching_db, on=['company_conf_id', 'competitor_conf_id'],
                                               how='inner')
        add_to_db = new_matching
        if ~existing_matching.empty:
            for index, row in existing_matching.iterrows():
                add_to_db = add_to_db.loc[(add_to_db['company_conf_id'] != row['company_conf_id']) & (
                        add_to_db['competitor_conf_id'] != row['competitor_conf_id'])]
        self.logger.info(f'New configs matching: {add_to_db}')
        add_to_db.to_sql('competitors_configs_matching', self.engine_db_competitor_configs, if_exists='append',
                         index=False)


with Flow('comparison_with_company') as comparison:
    logger = prefect.context.get("logger")
    company_data_transformer = Company(logger)
    company_data = company_data_transformer.get_data_from_company_db(company_data_transformer)
    company_data = company_data_transformer.transform_company_data(company_data_transformer, company_data)
    comparators = [Comparator('hostkey', logger), Comparator('timeweb', logger), Comparator('servers_ru', logger)]
    for comparator in comparators:
        new_matching = comparator.find_similar_configs(comparator, company_data)
        comparator.load_to_db(comparator, new_matching)
