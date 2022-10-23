from pyspark.sql import SparkSession, functions as f


from lib.utils import create_spark_config
from lib.logger import Log4j


if __name__ == '__main__':
    config = create_spark_config()
    spark = SparkSession.builder \
                        .config(conf=config) \
                        .getOrCreate()

    app_name = spark.sparkContext.appName
    logger = Log4j(spark)
    # logger.info("Started {} program.".format(app_name))
    print(app_name)

    flight_time_df = spark.read \
                          .format("parquet") \
                          .option("path", "data_source/flight-time.parquet") \
                          .load()

    # flight_time_df.show(2)

    print("Partitions before : {}".format(flight_time_df.rdd.getNumPartitions()))
    flight_time_df.groupby(f.spark_partition_id()).count().show()

    partitioned_df = flight_time_df.repartition(5)
    print("Partitions after : {}".format(partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupby(f.spark_partition_id()).count().show()

    partitioned_df.write \
                  .format("avro") \
                  .mode("overwrite") \
                  .option("path", "data_sink/avro/") \
                  .save()

    # logger.info("Finished {} program.".format(app_name))
    spark.stop()
