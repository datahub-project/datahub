import { EntityType, TestResult, TestResultType } from '@src/types.generated';
import { excludeTestsByCategories } from '../testUtils';

const SAMPLE_TEST_RESULT_1: TestResult = {
    test: {
        urn: 'sample_1',
        category: 'sample_1',
        definition: {},
        name: 'sample_1',
        __typename: 'Test',
        type: EntityType.Dataset,
        results: { failingCount: 0, passingCount: 0 },
    },
    type: TestResultType.Success,
};

const SAMPLE_TEST_RESULT_2: TestResult = {
    test: {
        urn: 'sample_2',
        category: 'sample_2',
        definition: {},
        name: 'sample_2',
        __typename: 'Test',
        type: EntityType.Dataset,
        results: { failingCount: 0, passingCount: 0 },
    },
    type: TestResultType.Success,
};

describe('excludeTestsByCategories', () => {
    it('should exlude tests with passed categories', () => {
        const result = excludeTestsByCategories([SAMPLE_TEST_RESULT_1, SAMPLE_TEST_RESULT_2], ['sample_1']);
        expect(result).toStrictEqual([SAMPLE_TEST_RESULT_2]);
    });

    it('should handle empty arrays of tests', () => {
        const result = excludeTestsByCategories([], ['sample_1']);
        expect(result).toStrictEqual([]);
    });

    it('should handle empty arrays of categories', () => {
        const result = excludeTestsByCategories([SAMPLE_TEST_RESULT_1, SAMPLE_TEST_RESULT_2], []);
        expect(result).toStrictEqual([SAMPLE_TEST_RESULT_1, SAMPLE_TEST_RESULT_2]);
    });
});
