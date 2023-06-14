import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from flows.comparison import Company, Comparator
from pandas.testing import assert_frame_equal


class TestCompany(unittest.TestCase):

    @patch('sqlalchemy.create_engine')
    def setUp(self, mock_create_engine):
        self.logger = unittest.mock.MagicMock()
        self.company = Company(self.logger)
        self.mock_engine = mock_create_engine.return_value

    def test_unpack_disks_company(self):
        disks = [{'size': 10, 'count': 2, 'type': 'hdd'},
                 {'size': 20, 'count': 1, 'type': 'ssd'},
                 {'size': 30, 'count': 1, 'type': 'nvme'}]
        hdd, ssd, nvme = self.company.unpack_disks_company(disks)
        self.assertEqual(hdd, 20)
        self.assertEqual(ssd, 20)
        self.assertEqual(nvme, 30)

    def test_delete_vendor(self):
        cpu_name = 'AMD Ryzen 9'
        cpu_name_without_vendor = self.company.delete_vendor(cpu_name)
        self.assertEqual(cpu_name_without_vendor, 'Ryzen 9')
        cpu_name = 'Intel Core i7'
        cpu_name_without_vendor = self.company.delete_vendor(cpu_name)
        self.assertEqual(cpu_name_without_vendor, 'Core i7')

    def test_get_data_cpu(self):
        cpu_name = 'Ryzen 9-5950X'
        cpu_parts = self.company.get_data_cpu(cpu_name)
        self.assertEqual(cpu_parts, ['RYZEN', '9', '5950X'])

    def test_transform_company_data(self):
        test_data = [
            {
                'id_config': {'en': 'Test Config 1'},
                'cpu_name': 'Intel Xeon E-2234',
                'cpu_count': 1,
                'cpu_base_freq': 3.5,
                'cores_per_cpu': 2,
                'gpu_name': 'GPU A2000',
                'gpu_count': 1,
                'ram': [{'size': 8, 'count': 1, 'type': 'DDR4'}],
                'disk': [{'size': 256, 'count': 1, 'type': 'SSD'}, {'size': 1, 'count': 1, 'type': 'NVME'}],
                'price': {'RUB': {'month': 10000}}
            }
        ]

        expected_data = {
            'id_config': ['Test Config 1'],
            'cpu_model_1': ['XEON'],
            'cpu_model_2': ['E'],
            'cpu_gen': ['2234'],
            'cpu_count': [1],
            'gpu': ['RTX A2000'],
            'gpu_count': [1],
            'cores': [2],
            'frequency': [3.5],
            'ram': [8],
            'ram_type': ['DDR4'],
            'hdd_size': [0],
            'ssd_size': [256],
            'nvme_size': [1],
            'price': [10000]
        }
        expected_df = pd.DataFrame(expected_data)
        expected_df = expected_df.astype(
            {'id_config': str, 'cpu_model_1': str, 'cpu_model_2': str, 'cpu_gen': str, 'cpu_count': int,
             'gpu': object, 'gpu_count': int, 'cores': int, 'frequency': float, 'ram': int, 'ram_type': str,
             'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'price': float})
        result_df = self.company.transform_company_data(test_data)
        assert_frame_equal(result_df, expected_df)


class TestComparator(unittest.TestCase):

    def setUp(self):
        self.comparator = Comparator(competitor="TestCompetitor", logger=MagicMock())

    def test_compare_disks(self):
        config = pd.Series({
            'nvme_size': 0,
            'ssd_size': 200,
            'hdd_size': 300,
        })
        similar = pd.DataFrame({
            'id_config': ['1', '2', '3'],
            'nvme_size': [0, 100, 150],
            'ssd_size': [150, 0, 250],
            'hdd_size': [250, 300, 350],
        })
        result = self.comparator.compare_disks(config, similar)
        assert result.iloc[0]['id_config'] == '1'
        assert result.iloc[0]['nvme_size'] == 0
        assert result.iloc[0]['ssd_size'] == 150
        assert result.iloc[0]['hdd_size'] == 250

    def test_compare_gen(self):
        config = pd.Series({
            'cpu_model_1': 'XEON',
            'cpu_model_2': 'SILVER',
            'cpu_gen': '1'
        })
        similar = pd.DataFrame({
            'cpu_model_1': ['XEON', 'XEON', 'XEON'],
            'cpu_model_2': ['SILVER', 'GOLD', 'OTHER'],
            'cpu_gen': ['1', '2', '3'],
        })
        result = self.comparator.compare_gen(config, similar)
        assert result.iloc[0]['cpu_model_2'] == 'SILVER'
        assert result.iloc[0]['cpu_gen'] == '1'

    def test_split_by_gpu(self):
        df = pd.DataFrame({
            'gpu_count': [0, 1, 2, 0],
        })
        df_not_gpu, df_gpu = self.comparator.split_by_gpu(df)
        assert len(df_not_gpu) == 2
        assert len(df_gpu) == 2

    def test_compare_configs(self):
        competitor_configs = pd.DataFrame({
            'cpu_model_1': ['XEON', 'XEON', 'XEON', 'XEON'],
            'cpu_model_2': ['SILVER', 'SILVER', 'GOLD', 'BRONZE'],
            'cpu_gen': ['5466', '1231', '5778', '4536'],
            'cpu_count': [1, 1, 2, 2],
            'cores': [4, 4, 8, 8],
            'ram': [16, 16, 32, 32],
            'ram_type': ['DDR4', 'DDR4', 'DDR3', 'None'],
            'hdd_size': [500, 500, 1000, 1000],
            'ssd_size': [0, 0, 128, 128],
            'nvme_size': [0, 0, 0, 0],
            'frequency': [2.0, 2.0, 2.5, 2.5]
        })

        company_config = pd.Series({
            'cpu_model_2': 'SILVER',
            'ram_type': 'DDR4',
            'cpu_model_1': 'XEON',
            'cpu_count': 1,
            'cores': 4,
            'cpu_gen': '5466',
            'ram': 16,
            'hdd_size': 500,
            'ssd_size': 0,
            'nvme_size': 0,
            'frequency': 2.0
        })

        result = self.comparator.compare_configs(competitor_configs, company_config)

        self.assertTrue(isinstance(result, pd.DataFrame), "Result is not a DataFrame")
        self.assertTrue('total_match' in result.columns, "Result does not contain 'total_match' column")
        assert result.iloc[0]['cpu_model_1'] == 'XEON'
        assert result.iloc[0]['cpu_model_2'] == 'SILVER'
        assert result.iloc[0]['cpu_gen'] == '5466'
        assert result.iloc[0]['ram'] == 16
        assert result.iloc[0]['ram_type'] == 'DDR4'
        assert result.iloc[0]['cores'] == 4
        assert result.iloc[0]['frequency'] == 2.0
        assert result.iloc[0]['nvme_size'] == 0
        assert result.iloc[0]['ssd_size'] == 0
        assert result.iloc[0]['hdd_size'] == 500

    def test_compare_gpu_configs(self):
        competitor_config = pd.DataFrame({
            'gpu': ['GTX 1660', 'GTX 1070', 'RTX 2070', 'TESLA T4'],
            'gpu_count': [2, 4, 1, 1]
        })
        company_config = {
            'gpu': 'GTX 1070',
            'gpu_count': 4
        }

        output = self.comparator.compare_gpu_configs(competitor_config, company_config)
        assert output.iloc[0]['gpu'] == 'GTX 1070'
        assert output.iloc[0]['gpu_count'] == 4
        assert output.iloc[0]['total_match'] == 1


if __name__ == '__main__':
    unittest.main()
