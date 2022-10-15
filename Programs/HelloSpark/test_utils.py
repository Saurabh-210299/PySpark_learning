from pyspark.sql import SparkSession
from lib.utils import load_csv_data, count_by_country, get_spark_app_config
from unittest import TestCase


class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.conf = get_spark_app_config()
        cls.spark = SparkSession.builder \
                                .config(conf=cls.conf) \
                                .getOrCreate()

    def test_DataFile_Loading(self):
        raw_data = load_csv_data(self.spark, "data/sample.csv")
        count = raw_data.count()
        self.assertEqual(count, 9, "Record count should be 9.")

    def test_country_count(self):
        raw_data = load_csv_data(self.spark, "data/sample.csv")
        country_count_list = count_by_country(raw_data).collect()
        country_dict = dict()

        for row in country_count_list:
            country_dict[row["Country"]] = row["count"]

        self.assertEqual(country_dict["United States"], 4, "Count for United States should be 4")
        self.assertEqual(country_dict["Canada"], 2, "Count for Canada should be 4")
        self.assertEqual(country_dict["United Kingdom"], 1, "Count for United Kingdom should be 1")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
