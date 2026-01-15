"""
Pytest test suite demonstrating PySpark DataFrame transformations.

This test can be run standalone with: pytest test_pyspark_transformations.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, avg, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for the test module.

    This fixture is module-scoped, meaning a single Spark session is created
    once and reused across all tests. This is more efficient than creating
    a new session for each test.

    Configuration notes:
    - master("local[*]"): Uses all available CPU cores. Essential for CI/local testing.
    - driver.memory (2g): Explicit heap limit prevents OOM surprises in CI environments.
    - shuffle.partitions (4): Controls task parallelism during groupBy/join operations.
    - maxResultSize (1g): CRITICAL - Prevents accidental .collect() on large results.
    - adaptive.enabled: Spark automatically optimizes shuffle partitions at runtime.
    """
    spark_session = (SparkSession.builder
        .appName("PySparkTransformationTest")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate())

    yield spark_session

    # Cleanup after all tests
    spark_session.stop()


@pytest.fixture
def sample_data(spark):
    """Create sample DataFrame for testing."""
    data = [
        ("Alice", "Engineering", 75000, 28),
        ("Bob", "Sales", 65000, 32),
        ("Charlie", "Engineering", 85000, 35),
        ("Diana", "Marketing", 70000, 29),
        ("Eve", "Sales", 72000, 31),
        ("Frank", "Engineering", 90000, 40),
    ]

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True),
    ])

    return spark.createDataFrame(data, schema)


class TestDataFrameTransformations:
    """Test suite for PySpark DataFrame transformations."""

    def test_basic_filter_transformation(self, sample_data):
        """Test filtering rows based on conditions."""
        # Filter employees with salary > 70000
        result = sample_data.filter(col("salary") > 70000)

        assert result.count() == 4

        # Verify all salaries are above 70000
        salaries = [row.salary for row in result.collect()]
        assert all(salary > 70000 for salary in salaries)

    def test_select_and_column_rename(self, sample_data):
        """Test selecting columns and renaming them."""
        result = sample_data.select(
            col("name").alias("employee_name"),
            col("department"),
            (col("salary") / 1000).alias("salary_in_k")
        )

        # Check schema
        assert "employee_name" in result.columns
        assert "salary_in_k" in result.columns
        assert "age" not in result.columns

        # Verify transformation
        first_row = result.first()
        assert first_row.salary_in_k == 75.0

    def test_add_new_column_with_transformation(self, sample_data):
        """Test adding a new computed column."""
        result = sample_data.withColumn(
            "salary_category",
            when(col("salary") >= 80000, "High")
            .when(col("salary") >= 70000, "Medium")
            .otherwise("Low")
        )

        assert "salary_category" in result.columns

        # Test specific cases
        charlie = result.filter(col("name") == "Charlie").first()
        assert charlie.salary_category == "High"

        bob = result.filter(col("name") == "Bob").first()
        assert bob.salary_category == "Low"

    def test_groupby_aggregation(self, sample_data):
        """Test groupBy with multiple aggregations."""
        result = sample_data.groupBy("department").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary"),
            spark_sum("salary").alias("total_salary")
        )

        # Check Engineering department stats
        engineering = result.filter(col("department") == "Engineering").first()
        assert engineering.employee_count == 3
        assert engineering.avg_salary == (75000 + 85000 + 90000) / 3
        assert engineering.total_salary == 250000

    def test_orderby_transformation(self, sample_data):
        """Test sorting DataFrame."""
        result = sample_data.orderBy(col("salary").desc())

        # Check if sorted correctly (highest salary first)
        rows = result.collect()
        assert rows[0].name == "Frank"
        assert rows[0].salary == 90000
        assert rows[-1].name == "Bob"
        assert rows[-1].salary == 65000

    def test_multiple_filters_chained(self, sample_data):
        """Test chaining multiple filter operations."""
        result = sample_data \
            .filter(col("age") >= 30) \
            .filter(col("department") != "Marketing") \
            .filter(col("salary") > 70000)

        assert result.count() == 3
        names = [row.name for row in result.collect()]
        assert set(names) == {"Charlie", "Eve", "Frank"}

    def test_distinct_values(self, sample_data):
        """Test getting distinct values from a column."""
        departments = sample_data.select("department").distinct()

        assert departments.count() == 3
        dept_list = [row.department for row in departments.collect()]
        assert set(dept_list) == {"Engineering", "Sales", "Marketing"}

    def test_join_transformation(self, spark):
        """Test joining two DataFrames."""
        # Create two DataFrames
        employees = spark.createDataFrame([
            (1, "Alice", "Engineering"),
            (2, "Bob", "Sales"),
            (3, "Charlie", "Engineering"),
        ], ["id", "name", "dept"])

        salaries = spark.createDataFrame([
            (1, 75000),
            (2, 65000),
            (3, 85000),
        ], ["emp_id", "salary"])

        # Perform join
        result = employees.join(
            salaries,
            employees.id == salaries.emp_id,
            "inner"
        )

        assert result.count() == 3
        assert "name" in result.columns
        assert "salary" in result.columns

    def test_udf_string_transformation(self, sample_data):
        """Test string transformations using built-in functions."""
        result = sample_data.withColumn(
            "name_upper",
            upper(col("name"))
        )

        first_row = result.first()
        assert first_row.name_upper == "ALICE"

    def test_null_handling(self, spark):
        """Test handling null values in transformations."""
        data = [
            ("Alice", 75000),
            ("Bob", None),
            ("Charlie", 85000),
        ]

        df = spark.createDataFrame(data, ["name", "salary"])

        # Filter out nulls
        result = df.filter(col("salary").isNotNull())
        assert result.count() == 2

        # Fill nulls with default value
        filled = df.fillna({"salary": 0})
        bob_salary = filled.filter(col("name") == "Bob").first().salary
        assert bob_salary == 0


class TestDataFramePersistence:
    """Test DataFrame caching and persistence strategies."""

    def test_cache_dataframe(self, sample_data):
        """Test caching a DataFrame."""
        # Cache the DataFrame
        cached_df = sample_data.cache()

        # Trigger an action to actually cache
        assert cached_df.count() == 6

        # Verify it's cached
        assert cached_df.is_cached

        # Perform another action (should use cache)
        result = cached_df.filter(col("salary") > 70000)
        assert result.count() == 4

        # Unpersist
        cached_df.unpersist()


if __name__ == "__main__":
    """Allow running as standalone script."""
    pytest.main([__file__, "-v", "--tb=short"])
