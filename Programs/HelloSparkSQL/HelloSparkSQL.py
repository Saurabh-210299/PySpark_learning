from pyspark.sql import SparkSession
from lib.utils import create_spark_app_config
from lib.logger import Log4j

if __name__ == '__main__':
    config = create_spark_app_config()
    spark = SparkSession.builder \
                        .config(conf=config) \
                        .getOrCreate()

    logger = Log4j(spark)
    app_name = spark.sparkContext.appName
    logger.info("Starting {} program".format(app_name))

    print(app_name)

    logger.info("Finished {} program".format(app_name))

    spark.stop()
