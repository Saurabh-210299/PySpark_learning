from pyspark import SparkConf
from configparser import ConfigParser


def create_spark_config():
    config = SparkConf()
    config_parser = ConfigParser()
    config_file_path = r"F:\PYTHON\COGNIZANT\PYSPARK_LEARNING\APACHE_SPARK" \
                       r"\PySpark_learning\Programs\SparkDataFrameWriter\spark.conf"
    config_parser.read(config_file_path)
    for key, value in config_parser.items("SPARK_APP_CONFIGS"):
        config.set(key, value)

    return config
