Objective

The purpose of this task was to evaluate the performance of open-source query engines while working with the Apache Iceberg table format. This included data ingestion, engine setup, and query performance benchmarking. The goal was to determine which engine performs better under similar workloads and conditions.
 Tools & Technologies Used

    Table Format: Apache Iceberg

    Query Engines:

        Apache Spark 

        DuckDB 

    Environment: Local machine / cloud instance (mention specifics if relevant: CPU, RAM, OS)

    Dataset: 6 millions records. Size 1.5Gb

Data Ingestion

Data was ingested into an Apache Iceberg table using both Spark and DuckDB-compatible formats:

    Spark: Used Iceberg's Spark integration to write data directly in Iceberg format.

        Configured catalog using Hadoop catalog or Hive catalog (whichever you used).

        Partitioned by effective_date

    DuckDB: Read the Iceberg table directly using DuckDB's support for Iceberg (via the read_iceberg function).

        Minimal configuration needed; very fast to set up.

Query Engine Setup
Spark Setup

    Used Spark with Iceberg connector.

    Queries were run via Spark SQL or PySpark.

    Required more setup, including session configuration, catalog setup, and memory tuning for optimal performance.

DuckDB Setup

    DuckDB was extremely lightweight.

    Minimal setup: Iceberg plugin and single command to query.

    All queries run in-process; no cluster setup required.

Benchmarking Process

    Defined a consistent set of queries

    Ran each query 5 times to get average execution times.

    Cleared cache between runs to avoid bias (or noted otherwise).

Results Summary are available as part of python files.

    Note: DuckDB outperformed Spark in all queries due to its in-process execution model and low overhead.

🔍 Analysis & Observations

    Performance: DuckDB was significantly faster in all benchmarks. Its execution engine is optimized for single-node performance and works especially well with analytical workloads.

    Ease of Use: DuckDB was easier to set up and use. Spark required more configuration and had more overhead.

    Scalability: Spark would likely perform better in distributed or large-scale environments, but for local or small-medium datasets, DuckDB is a better choice.

    Integration: Both engines supported Iceberg well, but DuckDB’s lightweight usage made it more developer-friendly for quick data analysis.

Conclusion

DuckDB proved to be a more efficient and user-friendly engine for querying Apache Iceberg tables in this benchmark. While Spark is a powerful distributed processing engine suited for large-scale ETL jobs, DuckDB offers better performance for interactive analytics on moderate-sized data, with less overhead.
📌 Recommendations

    Use DuckDB for local analytics, rapid prototyping, and ad-hoc querying over Iceberg.

    Use Spark in scenarios requiring distributed processing or large-scale data pipelines.
