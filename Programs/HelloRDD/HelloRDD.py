from pyspark.sql import *
from lib.utils import create_spark_config
from lib.logger import Log4j
import sys

if __name__ == '__main__':
    config = create_spark_config()
    spark = SparkSession.builder \
                        .config(conf=config) \
                        .getOrCreate()

    sc = spark.sparkContext

    logger = Log4j(spark)

    logger.info("Starting HelloRDD program")

    if len(sys.argv) != 2:
        logger.warn("Usage HelloRDD <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    for line in linesRDD.collect():
        print(line)

    input("Press enter to end\n")  # To hold the spark session from getting stop

    logger.info("Finished HelloRDD program")

    spark.stop()
