from pyspark.sql.types import *
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

    logger.info("Starting {} program.".format(app_name))

    print(app_name)

    # File paths
    csv_file_path = "data/flight-time.csv"
    json_file_path = "data/flight-time.json"
    parquet_file_path = "data/flight-time.parquet"

    # Schemas
    programmatic_schema = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    DDL_schema = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
                    ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
                    WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    # Reading CSV file using inferSchema
    csv_df = spark.read \
                  .format("csv") \
                  .option("header", "true") \
                  .option("inferSchema", "true") \
                  .option("mode", "FAILFAST") \
                  .load(csv_file_path)
    csv_df.show(2)
    logger.info("Reading CSV file using inferSchema")
    logger.info(csv_df.schema.simpleString())

    # Reading CSV file by using programmatic defined explicit schema
    csv_with_schema_df = spark.read \
                              .format("csv") \
                              .option("header", "true") \
                              .option("mode", "FAILFAST") \
                              .option("dateFormat", "M/d/y") \
                              .schema(programmatic_schema) \
                              .load(csv_file_path)
    csv_with_schema_df.show(2)
    logger.info("Reading CSV file by using programmatic defined explicit schema")
    logger.info(csv_with_schema_df.schema.simpleString())

    # Reading CSV file using DDL defined explicit schema
    csv_with_ddl_schema_df = spark.read \
                              .format("csv") \
                              .option("header", "true") \
                              .option("mode", "FAILFAST") \
                              .option("dateFormat", "M/d/y") \
                              .schema(DDL_schema) \
                              .load(csv_file_path)
    csv_with_ddl_schema_df.show(2)
    logger.info("Reading CSV file using DDL defined explicit schema")
    logger.info(csv_with_ddl_schema_df.schema.simpleString())

    # Reading Json file
    json_df = spark.read \
                   .format("json") \
                   .option("mode", "FAILFAST") \
                   .load(json_file_path)
    json_df.show(2)
    logger.info("Reading Json file")
    logger.info(json_df.schema.simpleString())

    # Reading Json file using programmatic defined explicit schema
    json_with_schema_df = spark.read \
                               .format("json") \
                               .option("mode", "FAILFAST") \
                               .option("dateFormat", "M/d/y") \
                               .schema(programmatic_schema) \
                               .load(json_file_path)
    json_with_schema_df.show(2)
    logger.info("Reading Json file using programmatic defined explicit schema")
    logger.info(json_with_schema_df.schema.simpleString())

    # Reading Json file using DDL defined explicit schema
    json_with_ddl_schema_df = spark.read \
                                   .format("json") \
                                   .option("mode", "FAILFAST") \
                                   .option("dateFormat", "M/d/y") \
                                   .schema(DDL_schema) \
                                   .load(json_file_path)
    json_with_ddl_schema_df.show(2)
    logger.info("Reading Json file using DDL defined explicit schema")
    logger.info(json_with_ddl_schema_df.schema.simpleString())

    # Reading Parquet file
    parquet_df = spark.read \
                      .format("parquet") \
                      .option("mode", "FAILFAST") \
                      .load(parquet_file_path)
    parquet_df.show(2)
    logger.info("Reading Parquet file")
    logger.info(parquet_df.schema.simpleString())

    logger.info("Finished {} program.".format(app_name))
    spark.stop()
