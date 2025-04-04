from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format


def create_spark_session():
    # Create Spark session with optimized memory settings
    spark = (SparkSession.builder
             .appName("ParquetToIceberg")
             # Catalog configuration
             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local.type", "hadoop")
             .config("spark.sql.catalog.local.warehouse",
                     "file:///home/ihor/PycharmProjects/TestTaskIcceberg/spark-warehouse")
             # Iceberg runtime JAR
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             # Spark configurations
             .config("spark.sql.defaultCatalog", "local")
             .config("spark.sql.catalogImplementation", "in-memory")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.default.parallelism", "4")
             .config("spark.driver.memory", "12g")
             .config("spark.executor.memory", "4g")
             .config("spark.memory.fraction", "0.8")
             .config("spark.memory.storageFraction", "0.3")
             .getOrCreate())
    return spark


def load_taxi_dadta(spark: SparkSession, source_path: str):
    df = spark.read.parquet(source_path)
    return df


def transform_taxi_data(df):
    res = df.withColumn(
        "effective_date",
        date_format(col("request_datetime"), "yyyy-MM-dd")
    )
    return res


def save_to_iceberg(df, table_name, partition_col="effective_date"):
    spark = df.sparkSession
    table_exists = spark.catalog.tableExists(table_name)

    if not table_exists:
        (df.writeTo(table_name)
         .partitionedBy(partition_col)
         .using("iceberg")
         .option("write.format", "parquet")
         .create())
    else:
        (df.writeTo(table_name)
         .using("iceberg")
         .overwrite())


def show_iceberg(spark, table_name):
    spark.sql(f"DESCRIBE TABLE {table_name}").show()
    df_read = spark.read.format("iceberg").load(table_name)
    df_read.show()


def main():
    input_file = "/home/ihor/PycharmProjects/TestTaskIcceberg/resources"
    table_name = "default.taxi_data"

    print("Initializing Spark session...")
    spark = create_spark_session()

    print("Reading Parquet file...")
    df = load_taxi_dadta(spark, input_file)

    print("Transforming data...")
    transformed_df = transform_taxi_data(df)

    print(f"Saving to Iceberg")
    save_to_iceberg(transformed_df, table_name)

    print("Showing table schema and sample data:")
    show_iceberg(spark, table_name)

    spark.stop()
    print("Process completed successfully!")


if __name__ == "__main__":
    main()
