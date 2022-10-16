from pyspark.sql import SparkSession
from lib.utils import create_spark_app_config, load_csv_data
from lib.logger import Log4j
from sys import argv, exit

if __name__ == '__main__':
    config = create_spark_app_config()
    spark = SparkSession.builder \
                        .config(conf=config) \
                        .getOrCreate()

    logger = Log4j(spark)

    if len(argv) != 2:
        logger.warn("Usage: HelloSparkSQL <filename>")
        exit(-1)

    app_name = spark.sparkContext.appName
    logger.info("Starting {} program".format(app_name))

    survey_data = load_csv_data(spark, argv[1])
    survey_data.createOrReplaceTempView("survey_tmp")
    # spark.sql("select * from survey_tmp").show()

    query = "select Country, count(1) as Count from survey_tmp where Age < 40 group by Country"
    country_count_list = spark.sql(query).rdd \
                              .map(lambda row: "Country: {} --> Count: {}".format(row.Country, row.Count)) \
                              .collect()
    for country_count in country_count_list:
        logger.info(country_count)

    input()  # To check the spark UI
    logger.info("Finished {} program".format(app_name))

    spark.stop()
