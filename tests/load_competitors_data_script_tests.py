import json
import unittest
import requests
from unittest.mock import patch, Mock, MagicMock
from pydantic import ValidationError
from load_competitors_data_script import DataProcessor, Timeweb, Servers_ru, Hostkey
from load_competitors_data_script import StructureJson_Timeweb, StructureServer_Timeweb, StructureJson_Hostkey, \
    Structure_hostkey, Structure_hostkey_sale


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.mock_logger = Mock()
        self.processor = DataProcessor(self.mock_logger)

    def test_extract_data_from_website(self):
        url = "http://example.com"
        with patch('requests.get') as mock_get:
            mock_get.return_value = Mock()
            self.processor.extract_data_from_website(url)
            mock_get.assert_called_with(url)

    def test_delete_vendor(self):
        self.assertEqual(self.processor.delete_vendor("AMD Ryzen 5"), "Ryzen 5")
        self.assertEqual(self.processor.delete_vendor("Intel Core i5"), "Core i5")
        self.assertEqual(self.processor.delete_vendor('AMD Ryzen 7 5800X'), 'Ryzen 7 5800X')
        self.assertEqual(self.processor.delete_vendor('Intel Xeon E5-1650v4'), 'Xeon E5-1650v4')
        self.assertEqual(self.processor.delete_vendor('Core i7-4790'), 'Core i7-4790')
        self.assertEqual(self.processor.delete_vendor('Intel Xeon E5-2630 v3'), 'Xeon E5-2630 v3')

    def test_get_cpu_count(self):
        self.assertEqual(["Ryzen 5", 2], self.processor.get_cpu_count("2x Ryzen 5"))
        self.assertEqual(["Ryzen 5", 1], self.processor.get_cpu_count("Ryzen 5"))
        self.assertEqual(['EPYC 7551', 2], self.processor.get_cpu_count('2xEPYC 7551'))
        self.assertEqual(['Xeon E5-1650v4', 1], self.processor.get_cpu_count('Xeon E5-1650v4'))
        self.assertEqual(['Xeon Gold 6348', 2], self.processor.get_cpu_count('2 x Xeon Gold 6348'))
        self.assertEqual(['Xeon E5-1650v4', 1], self.processor.get_cpu_count('Xeon E5-1650v4'))
        self.assertEqual(['Xeon E5-2630 v3', 1], self.processor.get_cpu_count('Xeon E5-2630 v3'))

    def test_get_data_cpu(self):
        self.assertEqual(self.processor.get_data_cpu("RYZEN 5-3600"), ["RYZEN", "5", "3600"])
        self.assertEqual(self.processor.get_data_cpu("CORE i5-6600K"), ["CORE", "I5", "6600K"])
        self.assertEqual(['RYZEN', '9', '3900X'], self.processor.get_data_cpu('Ryzen 9 3900X'))
        self.assertEqual(['XEON', 'E', '2288G'], self.processor.get_data_cpu('Xeon E-2288G'))
        self.assertEqual(['XEON', 'GOLD', '6240'], self.processor.get_data_cpu('Xeon Gold 6240'))
        self.assertEqual(['CORE', 'I9', '9900K'], self.processor.get_data_cpu('Core i9-9900k'))
        self.assertEqual(['XEON', 'E5', '2680V2'], self.processor.get_data_cpu('Xeon E5-2680v2'))
        self.assertEqual(['XEON', 'E5', '2630V3'], self.processor.get_data_cpu('Xeon E5-2630 v3'))


class TestTimeweb(unittest.TestCase):

    def setUp(self):
        self.logger = Mock()
        self.processor = DataProcessor(self.logger)
        self.timeweb = Timeweb(self.logger)

    def test_unpack_disks(self):
        disks_input_1 = "1 x 500 ГБ SSD + 1 x 1 ТБ HDD"
        expected_output_1 = (1000, 500, 0, None, 0)

        disks_input_2 = "2 x 1 ТБ SSD + 1 x GeForce GTX 1080"
        expected_output_2 = (0, 2000, 0, "GTX 1080", 1)

        disks_input_3 = "2 x 1 ТБ NVMe + 2 x 1 ТБ SSD"
        expected_output_3 = (0, 2000, 2000, None, 0)

        result = self.timeweb.unpack_disks(disks_input_1)
        self.assertEqual(result, expected_output_1)

        result = self.timeweb.unpack_disks(disks_input_2)
        self.assertEqual(result, expected_output_2)

        result = self.timeweb.unpack_disks(disks_input_3)
        self.assertEqual(result, expected_output_3)

    def test_transform_data(self):

        class MockResponse:
            def __init__(self, json_data):
                self.json_data = json_data

            def json(self):
                return self.json_data

        data_from_website_1 = MockResponse({
            "body": [
                {
                    "cpu_vendor_short": "AMD Ryzen 7 5768",
                    "cpu_cores": 8,
                    "cpu_vendor": "AMD Ryzen 7 5768, 3.2 ГГц",
                    "memory": 16000,
                    "memory_type": "DDR4 ECC",
                    "disk_desc": "2 x 512 ГБ NVMe",
                    "price": 1500
                },
                {
                    "cpu_vendor_short": "Intel Xeon E3-1236",
                    "cpu_cores": 4,
                    "cpu_vendor": "Intel Xeon E3, 2.6 ГГц",
                    "memory": 8000,
                    "memory_type": "DDR4",
                    "disk_desc": "1 x 1 ТБ HDD",
                    "price": 1000
                }
            ]
        })

        # Проверка основных свойств результата.
        result1 = self.timeweb.transform_data(data_from_website_1)
        self.assertEqual(result1.shape, (2, 18))
        self.assertEqual(result1['cpu_model_1'].tolist(), ['RYZEN', 'XEON'])
        self.assertEqual(result1['cpu_model_2'].tolist(), ['7', 'E3'])
        self.assertEqual(result1['cpu_gen'].tolist(), ['5768', '1236'])
        self.assertEqual(result1['cpu_count'].tolist(), [1, 1])
        self.assertEqual(result1['gpu'].tolist(), [None, None])
        self.assertEqual(result1['gpu_count'].tolist(), [0, 0])
        self.assertEqual(result1['cores'].tolist(), [8, 4])
        self.assertEqual(result1['frequency'].tolist(), [3.2, 2.6])
        self.assertEqual(result1['ram'].tolist(), [16, 8])
        self.assertEqual(result1['ram_type'].tolist(), ['DDR4', 'DDR4'])
        self.assertEqual(result1['hdd_size'].tolist(), [0, 1000])
        self.assertEqual(result1['ssd_size'].tolist(), [0, 0])
        self.assertEqual(result1['nvme_size'].tolist(), [1024, 0])
        self.assertEqual(result1['price'].tolist(), [1500, 1000])

    def test_extract_data_from_website(self):
        url = "http://example.com"
        with patch('requests.get') as mock_get:
            mock_get.return_value = Mock()
            self.processor.extract_data_from_website(url)
            mock_get.assert_called_with(url)

    def test_validate_json_correct(self):
        json_1 = json.dumps({"body": [
            {"cpu_vendor_short": "AMD Ryzen 7", "cpu_cores": 8, "cpu_vendor": "AMD Ryzen 7, 3.2 ГГц", "memory": 16000,
             "memory_type": "DDR4 ECC", "disk_desc": "2 x 512 ГБ NVMe", "price": 1500},
            {"cpu_vendor_short": "Intel Xeon E3-1236", "cpu_cores": 48, "cpu_vendor": "Intel Xeon E3-1236, 3.4 ГГц",
             "memory": 196000,
             "memory_type": "DDR4 ECC", "disk_desc": "2 x 512 ГБ SSD", "price": 34600}]})
        try:
            self.timeweb.validate_json(json_1, StructureJson_Timeweb)
            json_dict = json.loads(json_1)['body']
            for i in json_dict:
                self.timeweb.validate_json(json.dumps(i), StructureServer_Timeweb)
        except ValidationError:
            self.fail("validate_json raised ValidationError unexpectedly!")

    def test_validate_json_invalid(self):
        invalid_json_1 = json.dumps(
            {"data": [{"cpu_vendor_short": "AMD Ryzen 7", "cpu_cores": 8, "cpu_vendor": "AMD Ryzen 7, 3.2 ГГц", ' \
                         '"memory": 16000, "memory_type": "DDR4 ECC", "disk_desc": "2 x 512 ГБ NVMe", ' \
                         '"price": 1500}, {"cpu_vendor_short": "Intel Xeon E3-1236", "cpu_cores": 48, "cpu_vendor": "Intel Xeon ' \
                         'E3-1236, 3.4 ГГц", "memory": 196000, "memory_type": "DDR4 ECC", "disk_desc": "2 x 512 ГБ SSD", ' \
                         '"price": 34600}]})
        invalid_json_2 = json.dumps({"body": 1000})
        invalid_json_3 = json.dumps({"fake": "AMD Ryzen 7", "cpu_cores": 8, "cpu_vendor": "AMD Ryzen 7, 3.2 ГГц", ' \
                         '"memory": 16000, "memory_type": "DDR4 ECC", "disk_desc": "2 x 512 ГБ NVMe", ' \
                         '"price": 1500})
        invalid_json_4 = json.dumps(
            {"cpu_vendor_short": "AMD Ryzen 7", "cpu_cores": 8, "cpu_vendor": "AMD Ryzen 7, 3.2 ГГц", ' \
                         '"memory": "16000", "memory_type": "DDR4 ECC", "disk_desc": "2 x 512 ГБ NVMe", ' \
                         '"price": 1500})

        with self.assertRaises(SystemExit):
            self.timeweb.validate_json(invalid_json_1, StructureJson_Timeweb)
            self.timeweb.validate_json(invalid_json_2, StructureJson_Timeweb)
            self.timeweb.validate_json(invalid_json_3, StructureServer_Timeweb)
            self.timeweb.validate_json(invalid_json_4, StructureServer_Timeweb)


class TestHostkey(unittest.TestCase):
    def setUp(self):
        self.logger = Mock()
        self.processor = DataProcessor(self.logger)
        self.hostkey = Hostkey(self.logger)

    @patch('requests.get')
    def test_extract_data(self, mock_get):
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = b'{"mock_key": "mock_value"}'
        mock_get.return_value = mock_response

        # Создаем объект класса Hostkey с фиктивным (mock) logger'ом
        hostkey = Hostkey(logger=unittest.mock.MagicMock())

        # Подменяем URL'ы, которые хотим проверить
        hostkey.url = ['http://mock_url_1', 'http://mock_url_2']

        # Вызываем метод extract_data
        hostkey_data_from_website = hostkey.extract_data()

        # Убеждаемся, что для каждого URL был сделан запрос
        calls = [unittest.mock.call(url) for url in hostkey.url]
        mock_get.assert_has_calls(calls, any_order=True)

        # Убеждаемся, что результат содержит ответы для всех запросов
        self.assertEqual(len(hostkey_data_from_website), len(hostkey.url))
        for response in hostkey_data_from_website:
            self.assertEqual(response.text, '{"mock_key": "mock_value"}')

    def test_validate_json_correct_hostkey(self):
        json_1 = json.dumps({"response": [{
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E3-12xx*",
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "description": "16 Gb",
                    "number": 1,
                    "volume": 16},
                "hard_drive": {"description": "240Gb SSD"}}}]})

        try:
            self.hostkey.validate_json(json_1, StructureJson_Hostkey)
            json_dict = json.loads(json_1)['response']
            for i in json_dict:
                self.hostkey.validate_json(json.dumps(i), Structure_hostkey)
        except ValidationError:
            self.fail("validate_json raised ValidationError unexpectedly!")

    def test_validate_json_invalid_hostkey(self):
        incorrect_json_1 = json.dumps({"data": [{
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E3-12xx*",
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "description": "16 Gb",
                    "number": 1,
                    "volume": 16},
                "hard_drive": {"description": "240Gb SSD"}}}]})
        incorrect_json_2 = json.dumps({"data": {'fake': 'data'}})
        incorrect_json_3 = json.dumps({
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E3-12xx*",
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "description": "16 Gb",
                    "number": 1,
                    "volume": 16}}})
        incorrect_json_4 = json.dumps({"response": [{
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E3-12xx*",
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "description": "16 Gb",
                    "number": 1,
                    "volume": '16'},
                "hard_drive": {"description": "240Gb SSD"}}}]})
        with self.assertRaises(SystemExit):
            self.hostkey.validate_json(incorrect_json_1, Structure_hostkey)
            self.hostkey.validate_json(incorrect_json_2, Structure_hostkey)
            self.hostkey.validate_json(incorrect_json_3, Structure_hostkey)
            self.hostkey.validate_json(incorrect_json_4, Structure_hostkey)

    def test_validate_json_correct_hostkey_sale(self):
        json_1 = json.dumps({"response": [{
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E3-12xx*",
                    "number": 2,
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "type": "DDR4",
                    "volume": 16},
                "hard_drive": {"items": [{"name": "480Gb NVMe SSD", "volume": "480", "count": 1}]},
                "graphics": {"number": 1, "items": "RTX A5000"}}}]})

        try:
            self.hostkey.validate_json(json_1, StructureJson_Hostkey)
            json_dict = json.loads(json_1)['response']
            for i in json_dict:
                self.hostkey.validate_json(json.dumps(i), Structure_hostkey_sale)
        except ValidationError:
            self.fail("validate_json raised ValidationError unexpectedly!")

    def test_validate_json_invalid_hostkey_sale(self):
        incorrect_json_1 = json.dumps({
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E3-12xx*",
                    "number": 2,
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "type": "DDR4",
                    "volume": 16},
                "hard_drive": {"description": [{"name": "480Gb NVMe SSD", "volume": "480", "count": 1}]},
                "graphics": {"number": 1, "items": "RTX A5000"}}})
        incorrect_json_2 = json.dumps({"data": {'fake': 'data'}})
        incorrect_json_3 = {
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E3-12xx*",
                    "number": 2,
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "type": "DDR4",
                    "volume": 16},
                "hard_drive": {"items": [{"name": "480Gb NVMe SSD", "volume": "480", "count": 1}]}
                }}
        incorrect_json_4 = {
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 3500},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": 123,
                    "number": 2,
                    "number_cores": 4,
                    "items": [{"number_cores": 4, "line": "Xeon E3-12xx*", "ghz": "3.2"}]},
                "ram": {
                    "type": "DDR4",
                    "volume": 16},
                "hard_drive": {"items": [{"name": "480Gb NVMe SSD", "volume": "480", "count": 1}]},
                "graphics": {"number": 1, "items": "RTX A5000"}
            }}
        with self.assertRaises(SystemExit):
            self.hostkey.validate_json(incorrect_json_1, Structure_hostkey_sale)
            self.hostkey.validate_json(incorrect_json_2, Structure_hostkey_sale)
            self.hostkey.validate_json(incorrect_json_3, Structure_hostkey_sale)
            self.hostkey.validate_json(incorrect_json_4, Structure_hostkey_sale)

    def test_unpack_disks_hostkey(self):
        logger = MagicMock()
        hostkey = Hostkey(logger)
        hdd, ssd, nvme = hostkey.unpack_disks_hostkey("1tb hdd + 1tb ssd + 1tb nvme")
        self.assertEqual(hdd, 1000)
        self.assertEqual(ssd, 1000)
        self.assertEqual(nvme, 1000)

    def test_unpack_disks_hostkey_sale_gpu(self):
        logger = MagicMock()
        hostkey = Hostkey(logger)
        disks = [{"volume": "480", "count": "2", "name": "hdd"},
                 {"volume": "960", "count": "1", "name": "ssd"},
                 {"volume": "1000", "count": "2", "name": "nvme"}]
        hdd, ssd, nvme = hostkey.unpack_disks_hostkey_sale_gpu(disks)
        self.assertEqual(hdd, 960)
        self.assertEqual(ssd, 960)
        self.assertEqual(nvme, 2000)

    def test_transform_data_hostkey(self):

        class MockResponse:
            def __init__(self, json_data):
                self.json_data = json_data

            def json(self):
                return self.json_data

        data_from_website_1 = MockResponse({"response": [{
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 35000},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E5-2640v4",
                    "number": 1,
                    "number_cores": 10,
                    "items": [{"number_cores": 10, "line": "Xeon E5-2640v4", "ghz": "2.4","name": "Xeon E5-2640v4 2.4GHz (10 cores)"}]},
                "ram": {
                    "type": "DDR4",
                    "volume": 16},
                "hard_drive": {"description": "240Gb SSD"},
                }}]})

        result1 = self.hostkey.create_df_hostkey(data_from_website_1)
        self.assertEqual(result1.shape, (1, 18))
        self.assertEqual(result1['cpu_model_1'].tolist(), ['XEON'])
        self.assertEqual(result1['cpu_model_2'].tolist(), ['E5'])
        self.assertEqual(result1['cpu_gen'].tolist(), ['2640V4'])
        self.assertEqual(result1['cpu_count'].tolist(), [1])
        self.assertEqual(result1['gpu'].tolist(), [None])
        self.assertEqual(result1['gpu_count'].tolist(), [0])
        self.assertEqual(result1['cores'].tolist(), [10])
        self.assertEqual(result1['frequency'].tolist(), ['2.4'])
        self.assertEqual(result1['ram'].tolist(), [16])
        self.assertEqual(result1['ram_type'].tolist(), [None])
        self.assertEqual(result1['hdd_size'].tolist(), [0])
        self.assertEqual(result1['ssd_size'].tolist(), [240])
        self.assertEqual(result1['nvme_size'].tolist(), [0])
        self.assertEqual(result1['price'].tolist(), [35000])

    def test_transform_data_sale(self):

        class MockResponse:
            def __init__(self, json_data):
                self.json_data = json_data

            def json(self):
                return self.json_data

        data_from_website_1 = MockResponse({"response": [{
            "common": {"location": "NL", "conditions": {"items": [
                {"prices": {"eurbase": 45, "current": 35000},
                 "params": {"items_short_name": "priceMonthly"}}]}},
            "hardware": {
                "cpu": {
                    "description": "Xeon E5-2640v4",
                    "number": 1,
                    "number_cores": 10,
                    "items": [{"number_cores": 10, "line": "Xeon E5-2640v4", "ghz": "2.4",
                               "name": "Xeon E5-2640v4 2.4GHz (10 cores)"}]},
                "ram": {
                    "type": "DDR4",
                    "volume": 16},
                "hard_drive": {"items": [{"name": "480Gb NVMe SSD", "volume": "480", "count": 1}]},
                "graphics": {"number": 1, "items": "RTX A5000"}}}]})

        result1 = self.hostkey.sale_gpu_hostkey(data_from_website_1)
        self.assertEqual(result1.shape, (1, 18))
        self.assertEqual(result1['cpu_model_1'].tolist(), ['XEON'])
        self.assertEqual(result1['cpu_model_2'].tolist(), ['E5'])
        self.assertEqual(result1['cpu_gen'].tolist(), ['2640V4'])
        self.assertEqual(result1['cpu_count'].tolist(), [1])
        self.assertEqual(result1['gpu'].tolist(), ['RTX A5000'])
        self.assertEqual(result1['gpu_count'].tolist(), [1])
        self.assertEqual(result1['cores'].tolist(), [10])
        self.assertEqual(result1['frequency'].tolist(), ['2.4'])
        self.assertEqual(result1['ram'].tolist(), [16])
        self.assertEqual(result1['ram_type'].tolist(), ['DDR4'])
        self.assertEqual(result1['hdd_size'].tolist(), [0])
        self.assertEqual(result1['ssd_size'].tolist(), [0])
        self.assertEqual(result1['nvme_size'].tolist(), [480])
        self.assertEqual(result1['price'].tolist(), [35000])


class TestServers_ru(unittest.TestCase):
    def setUp(self):
        self.logger = MagicMock()
        self.servers_ru = Servers_ru(self.logger)

    def test_unpack_disks(self):
        self.assertEqual(self.servers_ru.unpack_disks("2 x 1.5 тб hdd | 1 x 512 гб ssd"), (3000.0, 512.0, 0))
        self.assertEqual(self.servers_ru.unpack_disks("2 x 1.5 тб hdd"), (3000.0, 0, 0))
        self.assertEqual(self.servers_ru.unpack_disks("1 x 512 гб ssd"), (0, 512.0, 0))


if __name__ == '__main__':
    unittest.main()
