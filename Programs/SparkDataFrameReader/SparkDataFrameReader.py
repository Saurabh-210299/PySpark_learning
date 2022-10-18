from lib.utils import create_spark_config
from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == '__main__':
    config = create_spark_config()
    spark = SparkSession.builder \
                        .config(conf=config) \
                        .getOrCreate()

    app_name = spark.sparkContext.appName
    logger = Log4j(spark)

    # logger.info("Starting {} program.".format(app_name))

    print(app_name)

    # logger.info("Finished {} program.".format(app_name))
    spark.stop()
