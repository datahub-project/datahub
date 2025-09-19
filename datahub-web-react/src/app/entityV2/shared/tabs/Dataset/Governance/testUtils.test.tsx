import {
    TEST_CATEGORIES_TO_EXCLUDE,
    excludeTestsByCategories,
    getFilteredTestResults,
} from '@app/entityV2/shared/tabs/Dataset/Governance/testUtils';
import { TestResult, TestResultType } from '@src/types.generated';

// Mock test data helpers
const createMockTest = (category: string) => ({
    __typename: 'Test' as const,
    category,
    name: `Test ${category}`,
    urn: `urn:li:test:${category.toLowerCase()}`,
    type: 'TEST' as any,
    definition: {} as any,
    results: {} as any,
});

const createMockTestResult = (category: string, type: TestResultType): TestResult => ({
    __typename: 'TestResult' as const,
    test: createMockTest(category),
    type,
});

const createMockTestResultWithNullTest = (type: TestResultType): TestResult => ({
    __typename: 'TestResult' as const,
    test: null,
    type,
});

describe('getFilteredTestResults', () => {
    describe('basic functionality', () => {
        it('should return filtered passing and failing tests with correct totals', () => {
            const passing: TestResult[] = [
                createMockTestResult('DataQuality', TestResultType.Success),
                createMockTestResult('Schema', TestResultType.Success),
                createMockTestResult('Forms', TestResultType.Success), // Should be excluded
            ];

            const failing: TestResult[] = [
                createMockTestResult('DataQuality', TestResultType.Failure),
                createMockTestResult('Performance', TestResultType.Failure),
                createMockTestResult('Forms', TestResultType.Failure), // Should be excluded
            ];

            const result = getFilteredTestResults(passing, failing);

            expect(result.filteredPassing).toHaveLength(2);
            expect(result.filteredFailing).toHaveLength(2);
            expect(result.totalTests).toBe(4);

            // Verify that 'Forms' category tests are excluded
            expect(result.filteredPassing.every((test) => test.test?.category !== 'Forms')).toBe(true);
            expect(result.filteredFailing.every((test) => test.test?.category !== 'Forms')).toBe(true);
        });

        it('should handle empty arrays', () => {
            const result = getFilteredTestResults([], []);

            expect(result.filteredPassing).toHaveLength(0);
            expect(result.filteredFailing).toHaveLength(0);
            expect(result.totalTests).toBe(0);
        });

        it('should handle only passing tests', () => {
            const passing: TestResult[] = [
                createMockTestResult('DataQuality', TestResultType.Success),
                createMockTestResult('Schema', TestResultType.Success),
            ];

            const result = getFilteredTestResults(passing, []);

            expect(result.filteredPassing).toHaveLength(2);
            expect(result.filteredFailing).toHaveLength(0);
            expect(result.totalTests).toBe(2);
        });

        it('should handle only failing tests', () => {
            const failing: TestResult[] = [
                createMockTestResult('DataQuality', TestResultType.Failure),
                createMockTestResult('Performance', TestResultType.Failure),
            ];

            const result = getFilteredTestResults([], failing);

            expect(result.filteredPassing).toHaveLength(0);
            expect(result.filteredFailing).toHaveLength(2);
            expect(result.totalTests).toBe(2);
        });
    });

    describe('null test handling', () => {
        it('should filter out test results with null tests', () => {
            const passing: TestResult[] = [
                createMockTestResult('DataQuality', TestResultType.Success),
                createMockTestResultWithNullTest(TestResultType.Success),
            ];

            const failing: TestResult[] = [
                createMockTestResult('Performance', TestResultType.Failure),
                createMockTestResultWithNullTest(TestResultType.Failure),
            ];

            const result = getFilteredTestResults(passing, failing);

            expect(result.filteredPassing).toHaveLength(1);
            expect(result.filteredFailing).toHaveLength(1);
            expect(result.totalTests).toBe(2);

            // Verify all returned tests have non-null test property
            expect(result.filteredPassing.every((test) => test.test !== null)).toBe(true);
            expect(result.filteredFailing.every((test) => test.test !== null)).toBe(true);
        });

        it('should handle arrays with only null tests', () => {
            const passing: TestResult[] = [
                createMockTestResultWithNullTest(TestResultType.Success),
                createMockTestResultWithNullTest(TestResultType.Success),
            ];

            const failing: TestResult[] = [createMockTestResultWithNullTest(TestResultType.Failure)];

            const result = getFilteredTestResults(passing, failing);

            expect(result.filteredPassing).toHaveLength(0);
            expect(result.filteredFailing).toHaveLength(0);
            expect(result.totalTests).toBe(0);
        });
    });

    describe('category exclusion', () => {
        it('should exclude tests from TEST_CATEGORIES_TO_EXCLUDE', () => {
            const passing: TestResult[] = TEST_CATEGORIES_TO_EXCLUDE.map((category) =>
                createMockTestResult(category, TestResultType.Success),
            );

            const failing: TestResult[] = TEST_CATEGORIES_TO_EXCLUDE.map((category) =>
                createMockTestResult(category, TestResultType.Failure),
            );

            const result = getFilteredTestResults(passing, failing);

            expect(result.filteredPassing).toHaveLength(0);
            expect(result.filteredFailing).toHaveLength(0);
            expect(result.totalTests).toBe(0);
        });

        it('should include tests not in excluded categories', () => {
            const allowedCategories = ['DataQuality', 'Schema', 'Performance'];
            const passing: TestResult[] = allowedCategories.map((category) =>
                createMockTestResult(category, TestResultType.Success),
            );

            const failing: TestResult[] = allowedCategories.map((category) =>
                createMockTestResult(category, TestResultType.Failure),
            );

            const result = getFilteredTestResults(passing, failing);

            expect(result.filteredPassing).toHaveLength(3);
            expect(result.filteredFailing).toHaveLength(3);
            expect(result.totalTests).toBe(6);
        });

        it('should handle mixed categories (some excluded, some included)', () => {
            const passing: TestResult[] = [
                createMockTestResult('DataQuality', TestResultType.Success),
                createMockTestResult('Forms', TestResultType.Success), // Excluded
                createMockTestResult('Schema', TestResultType.Success),
            ];

            const failing: TestResult[] = [
                createMockTestResult('Performance', TestResultType.Failure),
                createMockTestResult('Forms', TestResultType.Failure), // Excluded
            ];

            const result = getFilteredTestResults(passing, failing);

            expect(result.filteredPassing).toHaveLength(2);
            expect(result.filteredFailing).toHaveLength(1);
            expect(result.totalTests).toBe(3);

            // Verify excluded categories are not present
            const allResults = [...result.filteredPassing, ...result.filteredFailing];
            expect(
                allResults.every(
                    (test) => test.test?.category && !TEST_CATEGORIES_TO_EXCLUDE.includes(test.test.category),
                ),
            ).toBe(true);
        });
    });

    describe('edge cases', () => {
        it('should handle tests with undefined category', () => {
            const testWithUndefinedCategory: TestResult = {
                __typename: 'TestResult' as const,
                test: {
                    ...createMockTest('DataQuality'),
                    category: undefined as any,
                },
                type: TestResultType.Success,
            };

            const result = getFilteredTestResults([testWithUndefinedCategory], []);

            // Tests with undefined category should be filtered out by excludeTestsByCategories
            expect(result.filteredPassing).toHaveLength(0);
            expect(result.totalTests).toBe(0);
        });

        it('should handle large arrays efficiently', () => {
            const largePassingArray: TestResult[] = Array.from({ length: 1000 }, (_, i) =>
                createMockTestResult(`Category${i % 10}`, TestResultType.Success),
            );

            const largeFailingArray: TestResult[] = Array.from({ length: 500 }, (_, i) =>
                createMockTestResult(`Category${i % 5}`, TestResultType.Failure),
            );

            const result = getFilteredTestResults(largePassingArray, largeFailingArray);

            expect(result.filteredPassing.length).toBeGreaterThan(0);
            expect(result.filteredFailing.length).toBeGreaterThan(0);
            expect(result.totalTests).toBe(result.filteredPassing.length + result.filteredFailing.length);
        });
    });

    describe('integration with excludeTestsByCategories', () => {
        it('should properly use excludeTestsByCategories function', () => {
            const tests: TestResult[] = [
                createMockTestResult('DataQuality', TestResultType.Success),
                createMockTestResult('Forms', TestResultType.Success),
            ];

            // Test the excludeTestsByCategories function directly
            const filtered = excludeTestsByCategories(tests, ['Forms']);
            expect(filtered).toHaveLength(1);
            expect(filtered[0].test?.category).toBe('DataQuality');

            // Test that getFilteredTestResults uses this function correctly
            const result = getFilteredTestResults(tests, []);
            expect(result.filteredPassing).toHaveLength(1);
            expect(result.filteredPassing[0].test?.category).toBe('DataQuality');
        });
    });
});
