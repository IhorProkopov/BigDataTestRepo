import duckdb
import time
import psutil


def create_connection():
    # Create Spark session with optimized memory settings
    conn = duckdb.connect(database=':memory:')

    conn.execute("INSTALL iceberg; load iceberg;")
    conn.execute("SET unsafe_enable_version_guessing = true;")
    return conn


def make_simple_aggregation(conn):
    start_time = time.time()
    process = psutil.Process()

    # Measure initial memory usage
    mem_info_start = process.memory_info()
    df = conn.execute(
        "select sum(tips) from iceberg_scan('/home/ihor/PycharmProjects/TestTaskIcceberg/spark-warehouse/default/taxi_data')").fetchdf()
    print(df)

    # Measure final memory usage
    mem_info_end = process.memory_info()

    end_time = time.time()
    execution_time = end_time - start_time
    memory_usage = mem_info_end.rss - mem_info_start.rss

    print(f"Execution time: {execution_time} seconds")
    print(f"Memory usage: {memory_usage / (1024 * 1024)} MB")


def make_complex_aggregation(conn):
    start_time = time.time()
    process = psutil.Process()

    # Measure initial memory usage
    mem_info_start = process.memory_info()
    df = conn.execute(
        "select hvfhs_license_num, (sum(driver_pay) + sum(tolls) - sum(tolls)) as total_profit from iceberg_scan('/home/ihor/PycharmProjects/TestTaskIcceberg/spark-warehouse/default/taxi_data') group by hvfhs_license_num").fetchdf()
    print(df)

    # Measure final memory usage
    mem_info_end = process.memory_info()

    end_time = time.time()
    execution_time = end_time - start_time
    memory_usage = mem_info_end.rss - mem_info_start.rss

    print(f"Execution time: {execution_time} seconds")
    print(f"Memory usage: {memory_usage / (1024 * 1024)} MB")


def main():

    print("Initializing Connection...")
    connection = create_connection()

    print("Make simple aggregation...")
    make_simple_aggregation(connection)

    print("Make complex aggregation...")
    make_complex_aggregation(connection)

    print("Process completed successfully!")


if __name__ == "__main__":
    main()


# Initializing Connection..
# Make simple aggregation...
#       sum(tips)
# 0  6.575124e+07
# Execution time: 0.4294273853302002 seconds
# Memory usage: 102.1171875 MB
# Make complex aggregation...
#   hvfhs_license_num  total_profit
# 0            HV0003  8.520503e+08
# 1            HV0005  2.925734e+08
# Execution time: 0.31854248046875 seconds
# Memory usage: 15.125 MB
# Process completed successfully!