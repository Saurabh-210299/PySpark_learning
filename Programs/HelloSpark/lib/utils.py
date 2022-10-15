import configparser

from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config_file_path = r"F:\PYTHON\COGNIZANT\PYSPARK_LEARNING\APACHE_SPARK\Spark_learning\Programs\HelloSpark\spark" \
                       r".conf "
    config.read(config_file_path)

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def load_csv_data(spark, file):
    df = spark.read \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .csv(file)
    return df


def count_by_country(df):
    count_df = df.filter("Age < 40") \
                 .select("Age", "Gender", "Country", "state") \
                 .groupBy("Country") \
                 .count()
    return count_df
