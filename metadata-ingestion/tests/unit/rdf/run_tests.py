#!/usr/bin/env python3
"""
Test Runner for DataHub RDF Operations

This script runs all unit tests for the modular transpiler architecture.
"""

import sys
import unittest
from pathlib import Path

# Add the src directory to the Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Import all test modules
from test_datahub_exporter import TestDataHubExporter  # noqa: E402
from test_transpiler_architecture import TestTranspilerArchitecture  # noqa: E402


def create_test_suite():
    """Create a test suite with all test cases."""
    suite = unittest.TestSuite()

    # Add test cases from each module
    suite.addTest(unittest.makeSuite(TestDataHubExporter))
    suite.addTest(unittest.makeSuite(TestTranspilerArchitecture))

    return suite


def run_tests():
    """Run all tests with detailed output."""
    # Create test suite
    suite = create_test_suite()

    # Create test runner
    runner = unittest.TextTestRunner(verbosity=2, descriptions=True, failfast=False)

    # Run tests
    print("=" * 70)
    print("RUNNING UNIT TESTS FOR MODULAR DATAHUB RDF OPERATIONS")
    print("=" * 70)
    print()

    result = runner.run(suite)

    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(
        f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%"
    )

    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            print(
                f"  - {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}"
            )

    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('\\n')[-2]}")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
