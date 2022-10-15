import configparser

from pyspark import SparkConf


def create_spark_config():
    conf = SparkConf()
    config_parser = configparser.ConfigParser()
    config_path = r"F:\PYTHON\COGNIZANT\PYSPARK_LEARNING\APACHE_SPARK\PySpark_learning\Programs\HelloRDD\spark.conf"
    config_parser.read(config_path)

    for key, value in config_parser.items("SPARK_APP_CONFIG"):
        conf.set(key, value)

    return conf
