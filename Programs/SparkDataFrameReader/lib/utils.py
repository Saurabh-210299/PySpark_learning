from pyspark import SparkConf
from configparser import ConfigParser


def create_spark_config():
    conf = SparkConf()
    config_parser = ConfigParser()
    config_file_path = r"F:\PYTHON\COGNIZANT\PYSPARK_LEARNING\APACHE_SPARK" \
                       r"\PySpark_learning\Programs\SparkDataFrameReader\spark.conf"
    config_parser.read(config_file_path)
    for key, value in config_parser.items("SPARK_APP_CONFIGS"):
        conf.set(key, value)

    return conf
