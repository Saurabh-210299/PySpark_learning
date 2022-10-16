import configparser
from pyspark import SparkConf


def create_spark_app_config():
    config = SparkConf()
    config_parser = configparser.ConfigParser()
    config_file_path = r"F:\PYTHON\COGNIZANT\PYSPARK_LEARNING\APACHE_SPARK" \
                       r"\PySpark_learning\Programs\HelloSparkSQL\spark.conf"
    config_parser.read(config_file_path)
    for key, value in config_parser.items("SPARK_APP_CONFIGS"):
        config.set(key, value)

    return config


def load_csv_data(spark, filepath):
    df = spark.read \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .csv(filepath)

    return df
