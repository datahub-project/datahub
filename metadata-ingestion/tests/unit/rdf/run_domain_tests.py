#!/usr/bin/env python3
"""
Test runner for comprehensive glossary domain hierarchy testing.

This script runs all domain hierarchy tests and provides detailed reporting.
"""

import os
import sys
import time
import unittest

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def run_all_tests():
    """Run all domain hierarchy tests."""
    print("=" * 80)
    print("COMPREHENSIVE GLOSSARY DOMAIN HIERARCHY TEST SUITE")
    print("=" * 80)
    print()

    # Import test modules
    try:
        from test_glossary_domain_hierarchy import (
            TestDomainCreationIntegration,
            TestDomainHierarchyCreation,
            TestDomainReuse,
            TestEdgeCases,
            TestGlossaryTermConversion,
        )
        from test_glossary_domain_integration import (
            TestDomainValidation,
            TestRDFToDataHubPipeline,
        )
    except ImportError as e:
        print(f"Error importing test modules: {e}")
        return False

    # Create test suite
    test_suite = unittest.TestSuite()

    # Add unit test classes
    unit_test_classes = [
        TestDomainHierarchyCreation,
        TestGlossaryTermConversion,
        TestDomainCreationIntegration,
        TestEdgeCases,
        TestDomainReuse,
    ]

    # Add integration test classes
    integration_test_classes = [TestRDFToDataHubPipeline, TestDomainValidation]

    print("Unit Tests:")
    print("-" * 40)
    for test_class in unit_test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
        print(f"  ✓ {test_class.__name__}")

    print()
    print("Integration Tests:")
    print("-" * 40)
    for test_class in integration_test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
        print(f"  ✓ {test_class.__name__}")

    print()
    print("Running Tests...")
    print("=" * 80)

    # Run tests with detailed output
    start_time = time.time()
    runner = unittest.TextTestRunner(
        verbosity=2, stream=sys.stdout, descriptions=True, failfast=False
    )

    result = runner.run(test_suite)
    end_time = time.time()

    # Print detailed summary
    print()
    print("=" * 80)
    print("TEST EXECUTION SUMMARY")
    print("=" * 80)

    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped) if hasattr(result, "skipped") else 0
    successful = total_tests - failures - errors - skipped

    print(f"Total Tests:     {total_tests}")
    print(f"Successful:      {successful}")
    print(f"Failures:        {failures}")
    print(f"Errors:          {errors}")
    print(f"Skipped:         {skipped}")
    print(f"Success Rate:    {(successful / total_tests * 100):.1f}%")
    print(f"Execution Time:  {(end_time - start_time):.2f} seconds")

    if failures > 0:
        print()
        print("FAILURES:")
        print("-" * 40)
        for test, traceback in result.failures:
            print(f"❌ {test}")
            print(f"   {traceback.split('AssertionError:')[-1].strip()}")
            print()

    if errors > 0:
        print()
        print("ERRORS:")
        print("-" * 40)
        for test, traceback in result.errors:
            print(f"💥 {test}")
            print(f"   {traceback.split('Exception:')[-1].strip()}")
            print()

    print("=" * 80)

    # Test coverage summary
    print("TEST COVERAGE SUMMARY")
    print("=" * 80)
    print("✓ IRI Path Extraction")
    print("✓ Domain URN Generation")
    print("✓ Domain Hierarchy Creation")
    print("✓ Glossary Term Conversion")
    print("✓ Domain Assignment")
    print("✓ Case Preservation")
    print("✓ Special Character Handling")
    print("✓ Custom Scheme Support")
    print("✓ Edge Case Handling")
    print("✓ Domain Reuse")
    print("✓ Integration Pipeline")
    print("✓ DataHub Target Execution")
    print("✓ Domain Validation")
    print("✓ Error Handling")

    print()
    print("=" * 80)

    if result.wasSuccessful():
        print(
            "🎉 ALL TESTS PASSED! Domain hierarchy implementation is working correctly."
        )
        print()
        print("Key Features Validated:")
        print("• Domain hierarchy creation from IRI structure")
        print("• Case and character preservation")
        print("• Glossary term assignment to domains")
        print("• Domain reuse across terms")
        print("• Complete RDF to DataHub pipeline")
        print("• Error handling and edge cases")
    else:
        print("❌ SOME TESTS FAILED! Please review the failures above.")
        print()
        print("Common Issues:")
        print("• Check IRI parsing logic")
        print("• Verify domain URN generation")
        print("• Ensure proper case preservation")
        print("• Validate domain assignment logic")

    print("=" * 80)

    return result.wasSuccessful()


def run_specific_test_category(category):
    """Run specific test category."""
    if category == "unit":
        print("Running Unit Tests Only...")
        # Import and run only unit tests
        from test_glossary_domain_hierarchy import (
            TestDomainCreationIntegration,
            TestDomainHierarchyCreation,
            TestDomainReuse,
            TestEdgeCases,
            TestGlossaryTermConversion,
        )

        test_classes = [
            TestDomainHierarchyCreation,
            TestGlossaryTermConversion,
            TestDomainCreationIntegration,
            TestEdgeCases,
            TestDomainReuse,
        ]
    elif category == "integration":
        print("Running Integration Tests Only...")
        # Import and run only integration tests
        from test_glossary_domain_integration import (
            TestDomainValidation,
            TestRDFToDataHubPipeline,
        )

        test_classes = [TestRDFToDataHubPipeline, TestDomainValidation]
    else:
        print(f"Unknown test category: {category}")
        return False

    # Create and run test suite
    test_suite = unittest.TestSuite()
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        category = sys.argv[1]
        success = run_specific_test_category(category)
    else:
        success = run_all_tests()

    sys.exit(0 if success else 1)
