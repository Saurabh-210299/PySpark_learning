import os
import sys
from pyspark.sql import *
from lib.utils import create_spark_config
from lib.logger import Log4j
from collections import namedtuple

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])


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
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    for data in colsRDD.collect():
        print(data)

    input("Press enter to end\n")  # To hold the spark session from getting stop

    logger.info("Finished HelloRDD program")

    spark.stop()
