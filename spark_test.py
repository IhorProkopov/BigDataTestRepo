from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import time
import psutil


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
             .config("spark.driver.memory", "16g")
             .config("spark.executor.memory", "16g")
             .config("spark.sql.shuffle.spill", "true")
             .config("spark.sql.inMemoryColumnarStorage.batchSize", "1000")
             .config("spark.memory.fraction", "0.6")
             .config("spark.memory.storageFraction", "0.2")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .getOrCreate())
    return spark


def load_iceberg(spark, table_name):
    spark.sql(f"DESCRIBE TABLE {table_name}").show()
    df = spark.read.format("iceberg").load(table_name).na.fill(
        {"trip_miles": 0, "tips": 0, "driver_pay": 0, "tolls": 0})
    df.show()
    return df


def make_simple_aggregation(df):
    start_time = time.time()
    process = psutil.Process()

    # Measure initial memory usage
    mem_info_start = process.memory_info()
    print(df.count())
    res = df.groupBy("effective_date").agg(
        sum("trip_miles").alias("total_trip_miles"),
    )
    res.show()

    # Measure final memory usage
    mem_info_end = process.memory_info()

    end_time = time.time()
    execution_time = end_time - start_time
    memory_usage = mem_info_end.rss - mem_info_start.rss

    print(f"Execution time: {execution_time} seconds")
    print(f"Memory usage: {memory_usage / (1024 * 1024)} MB")


def make_complex_aggregation(df):
    start_time = time.time()
    process = psutil.Process()

    # Measure initial memory usage
    mem_info_start = process.memory_info()
    print(df.count())
    res = df.filter(col("tips") > 0).withColumn("profit", col("driver_pay") + col("tips") - col("tolls")).groupBy(
        "hvfhs_license_num").agg(
        sum("profit").alias("total_profit"),
    )
    res.show()

    # Measure final memory usage
    mem_info_end = process.memory_info()

    end_time = time.time()
    execution_time = end_time - start_time
    memory_usage = mem_info_end.rss - mem_info_start.rss

    print(f"Execution time: {execution_time} seconds")
    print(f"Memory usage: {memory_usage / (1024 * 1024)} MB")


def main():
    table_name = "default.taxi_data"

    print("Initializing Spark session...")
    spark = create_spark_session()

    print("Load data from Iceberg...")
    df = load_iceberg(spark, table_name)

    print("Make simple aggregation...")
    make_simple_aggregation(df)

    print("Make complex aggregation...")
    make_complex_aggregation(df)

    spark.stop()
    print("Process completed successfully!")


if __name__ == "__main__":
    main()


# Initializing Spark session...
# Load data from Iceberg...
# Make simple aggregation...
# Execution time: 0.6783976553302002 seconds
# Memory usage: 168.5829576 MB
# Make complex aggregation...
# Execution time: 0.41275849304567 seconds
# Memory usage: 195.651 MB
# Process completed successfully!