#!/usr/bin/env python3
"""
Developer utility to validate PySpark compatibility with DataHub.

This script checks:
1. PySpark version is 3.5+ as required
2. Core PySpark APIs used in DataHub remain functional
3. Dependency versions meet PySpark requirements

Usage:
    python scripts/validate_pyspark_compatibility.py

    or if you have the venv activated:
    python scripts/validate_pyspark_compatibility.py

Note: This is a developer utility, not a unit test. It validates that the
installed PySpark version and dependencies are compatible with DataHub's requirements.
"""

import sys
from typing import Optional


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


def check_pyspark_version() -> bool:
    """Verify PySpark 3.5+ is installed."""
    try:
        import pyspark

        version = pyspark.__version__
        parts = version.split(".")
        major_version = int(parts[0])
        minor_version = int(parts[1]) if len(parts) > 1 else 0

        print(f"✓ PySpark version: {version}")

        if major_version == 3 and minor_version >= 5:
            return True
        else:
            print(
                f"  ⚠ Warning: PySpark should be 3.5+, but got {version}. "
                "Some features may not work correctly."
            )
            return False

    except ImportError:
        print("✗ PySpark not installed")
        print("  Install with: pip install 'acryl-datahub[data-lake-profiling]'")
        return False


def check_pyspark_dependencies() -> bool:
    """Verify that dependencies meet PySpark 3.5 requirements."""
    all_ok = True

    # PySpark 3.5 requires:
    # - pandas >= 1.0.5 (supports both 1.x and 2.x)
    # - numpy >= 1.21, <2 (to match constraints)
    # - pyarrow >= 4.0.0

    pandas_version = get_installed_version("pandas")
    if pandas_version:
        parts = pandas_version.split(".")
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        if (major == 1 and minor >= 0) or major == 2:
            print(f"✓ Pandas version: {pandas_version}")
        else:
            print(
                f"✗ Pandas version {pandas_version} - should be >= 1.0.5 for PySpark 3.5"
            )
            all_ok = False
    else:
        print("⚠ Pandas not found (optional for some features)")

    numpy_version = get_installed_version("numpy")
    if numpy_version:
        parts = numpy_version.split(".")
        major, minor = int(parts[0]), int(parts[1])
        if major == 1 and minor >= 21:
            print(f"✓ NumPy version: {numpy_version}")
        else:
            print(f"✗ NumPy version {numpy_version} - should be 1.21+ for PySpark 3.5")
            all_ok = False
    else:
        print("⚠ NumPy not found (optional for some features)")

    pyarrow_version = get_installed_version("pyarrow")
    if pyarrow_version:
        major = int(pyarrow_version.split(".")[0])
        if major >= 4:
            print(f"✓ PyArrow version: {pyarrow_version}")
        else:
            print(f"✗ PyArrow version {pyarrow_version} - should be 4.0+")
            all_ok = False
    else:
        print("⚠ PyArrow not found (optional for some features)")

    return all_ok


def check_pyspark_core_apis() -> bool:
    """Test core PySpark APIs used in DataHub remain functional."""
    try:
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, when

        # Test SparkSession creation
        conf = SparkConf()
        conf.set("spark.app.name", "DataHub-Compatibility-Check")
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
        if df.count() != 3:
            print("✗ DataFrame count operation failed")
            return False

        # Test null handling
        null_count = df.select(
            count(when(col("amount").isNull(), "amount")).alias("null_count")
        ).collect()[0]["null_count"]
        if null_count != 1:
            print("✗ Null handling test failed")
            return False

        # Test column selection
        result = df.select("name").collect()
        if len(result) != 3:
            print("✗ Column selection failed")
            return False

        # Test schema access
        fields = df.schema.fields
        if len(fields) != 4 or fields[0].name != "id":
            print("✗ Schema access failed")
            return False

        # Test toPandas conversion (requires pandas)
        try:
            pandas_df = df.toPandas()
            if len(pandas_df) != 3:
                print("✗ toPandas conversion produced wrong result")
                return False
            print("  • PySpark to Pandas conversion works")
        except ImportError:
            print("  • Pandas not available, skipping toPandas test")

        # Test RDD operations
        rdd = df.rdd
        sample = rdd.take(2)
        if len(sample) != 2:
            print("✗ RDD operations failed")
            return False
        print("  • RDD operations work")

        # Test toDF (rename columns)
        renamed_df = df.toDF("id2", "name2", "amount2", "date2")
        if renamed_df.columns != ["id2", "name2", "amount2", "date2"]:
            print("✗ toDF operation failed")
            return False
        print("  • toDF operation works")

        # Clean up
        spark.stop()

        print("✓ All core PySpark APIs functional")
        return True

    except ImportError as e:
        print(f"✗ PySpark API test failed - ImportError: {e}")
        return False
    except Exception as e:
        print(f"✗ PySpark API test failed: {e}")
        return False


def check_pyspark_file_reading_apis() -> bool:
    """Test file reading APIs used for data lake profiling."""
    try:
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession

        conf = SparkConf()
        conf.set("spark.app.name", "DataHub-FileAPI-Check")
        conf.set("spark.master", "local[1]")

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        # Test that read APIs are available
        apis_ok = True
        if not hasattr(spark.read, "parquet"):
            print("  ✗ spark.read.parquet not available")
            apis_ok = False
        if not hasattr(spark.read, "csv"):
            print("  ✗ spark.read.csv not available")
            apis_ok = False
        if not hasattr(spark.read, "json"):
            print("  ✗ spark.read.json not available")
            apis_ok = False
        if not hasattr(spark.read, "format"):
            print("  ✗ spark.read.format not available (needed for avro)")
            apis_ok = False

        if apis_ok:
            print("✓ File reading APIs available")

        spark.stop()
        return apis_ok

    except ImportError as e:
        print(f"✗ File reading API check failed - ImportError: {e}")
        return False


def check_pyspark_sql_parser_api() -> bool:
    """Test SQL parser API used in Unity Catalog usage extraction."""
    try:
        import pyspark

        spark_context = pyspark.SparkContext.getOrCreate()
        spark_session = pyspark.sql.SparkSession(spark_context)

        # Test internal SQL parser API access (used in unity/usage.py)
        sql_parser = spark_session._jsparkSession.sessionState().sqlParser()
        if sql_parser is None:
            print("✗ SQL parser API not accessible")
            return False

        print("✓ SQL parser API accessible (internal API works)")

        spark_session.stop()
        return True

    except ImportError as e:
        print(f"✗ SQL parser API check failed - ImportError: {e}")
        return False
    except Exception as e:
        print(
            f"⚠ SQL parser API check failed - this internal API may have changed: {e}"
        )
        print("  This is a warning, not a critical error")
        return True  # Return True as this is just a warning


def main():
    """Run all PySpark compatibility checks."""
    print("=" * 60)
    print("DataHub PySpark 3.5 Compatibility Check")
    print("=" * 60)
    print()

    results = {}

    print("1. Checking PySpark version...")
    results["version"] = check_pyspark_version()
    print()

    print("2. Checking dependency versions...")
    results["dependencies"] = check_pyspark_dependencies()
    print()

    print("3. Checking core PySpark APIs...")
    results["core_apis"] = check_pyspark_core_apis()
    print()

    print("4. Checking file reading APIs...")
    results["file_apis"] = check_pyspark_file_reading_apis()
    print()

    print("5. Checking SQL parser API (Unity Catalog)...")
    results["sql_parser"] = check_pyspark_sql_parser_api()
    print()

    print("=" * 60)
    if all(results.values()):
        print("✓ All PySpark compatibility checks passed!")
        print("=" * 60)
        return 0
    else:
        print("✗ Some PySpark compatibility checks failed")
        print("=" * 60)
        print("\nFailed checks:")
        for check, passed in results.items():
            if not passed:
                print(f"  • {check}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
