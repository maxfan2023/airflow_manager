from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


CREATE_TABLE_SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table_fqn} (
    course_id INT,
    course_name STRING,
    instructor STRING
)
STORED AS PARQUET
"""

TRUNCATE_TABLE_SQL_TEMPLATE = """
TRUNCATE TABLE {table_fqn}
"""

INSERT_DATA_SQL_TEMPLATE = """
INSERT INTO TABLE {table_fqn}
VALUES
    (101, 'Airflow Basics', 'Max'),
    (102, 'Spark SQL Intro', 'Alice'),
    (103, 'PySpark Practice', 'Bob')
"""

QUERY_SQL_TEMPLATE = """
SELECT course_id, course_name, instructor
FROM {table_fqn}
ORDER BY course_id
"""


def run_create_table(spark: SparkSession, table_fqn: str) -> None:
    spark.sql(CREATE_TABLE_SQL_TEMPLATE.format(table_fqn=table_fqn))
    print(f"[OK] Table ensured: {table_fqn}")


def run_insert_data(spark: SparkSession, table_fqn: str) -> None:
    spark.sql(TRUNCATE_TABLE_SQL_TEMPLATE.format(table_fqn=table_fqn))
    spark.sql(INSERT_DATA_SQL_TEMPLATE.format(table_fqn=table_fqn))
    count = spark.sql(f"SELECT COUNT(1) AS cnt FROM {table_fqn}").collect()[0]["cnt"]
    print(f"[OK] Inserted records into {table_fqn}, current row count={count}")


def run_query_and_print(spark: SparkSession, table_fqn: str) -> None:
    df = spark.sql(QUERY_SQL_TEMPLATE.format(table_fqn=table_fqn))
    row_count = df.count()
    print(f"[OK] Read {row_count} rows from {table_fqn}")
    df.show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="PySpark Hive smoke test job")
    parser.add_argument("--action", required=True, choices=["create", "insert", "query"])
    parser.add_argument("--table", default="default.max_hive_testing_courses")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("pyspark_hive_smoke_test_job").enableHiveSupport().getOrCreate()
    try:
        if args.action == "create":
            run_create_table(spark, args.table)
        elif args.action == "insert":
            run_insert_data(spark, args.table)
        elif args.action == "query":
            run_query_and_print(spark, args.table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
