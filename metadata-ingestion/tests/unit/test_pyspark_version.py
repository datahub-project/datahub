"""
Test to validate PySpark 3.5 is being used and core APIs remain functional.

This test ensures that:
1. PySpark version is 3.5 or higher (for feature flag branch)
2. Core PySpark APIs used in DataHub remain compatible
3. Dependency versions meet PySpark 3.5 requirements
"""

import sys
from typing import Optional

import pytest


def get_installed_version(package_name: str) -> Optional[str]:
    """Get the installed version of a package."""
    try:
        if sys.version_info >= (3, 8):
            from importlib.metadata import version

            return version(package_name)
        else:
            import pkg_resources

            return pkg_resources.get_distribution(package_name).version
    except Exception:
        return None


@pytest.mark.integration
def test_pyspark_version():
    """Verify PySpark 3.5+ is installed (PySpark 4.0 upgrade is work in progress)."""
    try:
        import pyspark

        version = pyspark.__version__
        parts = version.split(".")
        major_version = int(parts[0])
        minor_version = int(parts[1]) if len(parts) > 1 else 0

        # This branch supports PySpark 3.5.x
        assert major_version == 3 and minor_version >= 5, (
            f"PySpark version should be 3.5+, but got {version}"
        )
        print(f"✓ PySpark version: {version}")
    except ImportError:
        pytest.skip("PySpark not installed - skipping version test")


@pytest.mark.integration
def test_pyspark_dependencies():
    """Verify that dependencies meet PySpark 3.5 requirements."""
    # PySpark 3.5 requires:
    # - pandas >= 1.0.5 (supports both 1.x and 2.x)
    # - numpy >= 1.21, <2 (to match constraints)
    # - pyarrow >= 4.0.0

    pandas_version = get_installed_version("pandas")
    if pandas_version:
        parts = pandas_version.split(".")
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        # PySpark 3.5 requires pandas >= 1.0.5 and supports both 1.x and 2.x
        assert (major == 1 and minor >= 0) or major == 2, (
            f"Pandas should be >= 1.0.5 for PySpark 3.5, but got {pandas_version}"
        )
        print(f"✓ Pandas version: {pandas_version}")

    numpy_version = get_installed_version("numpy")
    if numpy_version:
        parts = numpy_version.split(".")
        major, minor = int(parts[0]), int(parts[1])
        assert major == 1 and minor >= 21, (
            f"NumPy should be 1.21+ for PySpark 3.5, but got {numpy_version}"
        )
        print(f"✓ NumPy version: {numpy_version}")

    pyarrow_version = get_installed_version("pyarrow")
    if pyarrow_version:
        major = int(pyarrow_version.split(".")[0])
        assert major >= 4, f"PyArrow should be 4.0+, but got {pyarrow_version}"
        print(f"✓ PyArrow version: {pyarrow_version}")


@pytest.mark.integration
def test_pyspark_core_apis():
    """Test core PySpark APIs used in DataHub remain functional."""
    try:
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, when

        # Test SparkSession creation
        conf = SparkConf()
        conf.set("spark.app.name", "DataHub-PySpark4.0-Test")
        conf.set("spark.master", "local[1]")
        conf.set("spark.driver.memory", "1g")

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        # Test DataFrame creation and operations
        data = [
            (1, "Alice", 100.5, "2024-01-01"),
            (2, "Bob", 200.3, "2024-01-02"),
            (3, "Charlie", None, "2024-01-03"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "amount", "date"])

        # Test count operation
        assert df.count() == 3

        # Test null handling with isnan and isNull
        null_count = df.select(
            count(when(col("amount").isNull(), "amount")).alias("null_count")
        ).collect()[0]["null_count"]
        assert null_count == 1

        # Test column selection
        result = df.select("name").collect()
        assert len(result) == 3

        # Test schema access
        fields = df.schema.fields
        assert len(fields) == 4
        assert fields[0].name == "id"

        # Test toPandas conversion (requires pandas)
        try:
            pandas_df = df.toPandas()
            assert len(pandas_df) == 3
            print("✓ PySpark to Pandas conversion works")
        except ImportError:
            print("⚠ Pandas not available, skipping toPandas test")

        # Test RDD operations
        rdd = df.rdd
        sample = rdd.take(2)
        assert len(sample) == 2
        print("✓ RDD operations work")

        # Test toDF (rename columns)
        renamed_df = df.toDF("id2", "name2", "amount2", "date2")
        assert renamed_df.columns == ["id2", "name2", "amount2", "date2"]
        print("✓ toDF operation works")

        # Clean up
        spark.stop()

        print("✓ All core PySpark APIs functional with version 3.5+")

    except ImportError as e:
        pytest.skip(f"PySpark not installed - skipping API test: {e}")
    except Exception as e:
        pytest.fail(f"PySpark API test failed: {e}")


@pytest.mark.integration
def test_pyspark_file_reading_apis():
    """Test file reading APIs used for data lake profiling."""
    try:
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession

        conf = SparkConf()
        conf.set("spark.app.name", "DataHub-FileAPI-Test")
        conf.set("spark.master", "local[1]")

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        # Test that read APIs are available
        assert hasattr(spark.read, "parquet")
        assert hasattr(spark.read, "csv")
        assert hasattr(spark.read, "json")
        assert hasattr(spark.read, "format")  # For avro

        print("✓ File reading APIs available")

        spark.stop()

    except ImportError:
        pytest.skip("PySpark not installed - skipping file API test")


@pytest.mark.integration
def test_pyspark_sql_parser_api():
    """Test SQL parser API used in Unity Catalog usage extraction."""
    try:
        import pyspark

        spark_context = pyspark.SparkContext.getOrCreate()
        spark_session = pyspark.sql.SparkSession(spark_context)

        # Test internal SQL parser API access
        # This is used in unity/usage.py
        sql_parser = spark_session._jsparkSession.sessionState().sqlParser()
        assert sql_parser is not None

        print("✓ SQL parser API accessible (internal API still works)")

        spark_session.stop()

    except ImportError:
        pytest.skip("PySpark not installed - skipping SQL parser test")
    except Exception as e:
        pytest.fail(
            f"SQL parser API test failed - this internal API may have changed: {e}"
        )


if __name__ == "__main__":
    # Allow running this test file directly for quick validation
    print("PySpark 3.5 Compatibility Test\n" + "=" * 50)

    test_pyspark_version()
    test_pyspark_dependencies()
    test_pyspark_core_apis()
    test_pyspark_file_reading_apis()
    test_pyspark_sql_parser_api()

    print("\n" + "=" * 50)
    print("All PySpark 3.5 compatibility tests passed!")
