import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_csv_data, count_by_country

if __name__ == '__main__':

    """
    spark = SparkSession.builder \
                        .appName("Hello Spark") \
                        .master("local[3]") \
                        .getOrCreate()
    """
    conf = get_spark_app_config()
    spark = SparkSession.builder \
                        .config(conf=conf) \
                        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting Hello Spark")

    if len(sys.argv) != 2:
        logger.warn("Use: HelloSpark filename")
        sys.exit(-1)

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    raw_data = load_csv_data(spark, sys.argv[1])
    partitioned_data = raw_data.repartition(2)
    # We partitioned the data to simulate cluster partition

    count_df = count_by_country(partitioned_data)

    # print(count_df.rdd.getNumPartitions())

    logger.info(count_df.collect())

    input("Hit Enter to end the program")  # added this input so that sparkSession doesn't end and we can see spark UI

    logger.info("Finished Hello Spark")

    spark.stop()
