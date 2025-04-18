<<<<<<< HEAD
import { afterAll, beforeEach, describe, expect, test, vi } from 'vitest';

import { getEntitiesIngestedByType, removeEmptyArrays } from '@app/ingest/source/utils';

import { ExecutionRequestResult } from '@types';
=======
import { vi, describe, test, expect, beforeEach, afterAll } from 'vitest';
import { getEntitiesIngestedByType } from '../utils';
import { ExecutionRequestResult } from '../../../../types.generated';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

// Mock the structuredReport property of ExecutionRequestResult
const mockExecutionRequestResult = (structuredReportData: any): Partial<ExecutionRequestResult> => {
    return {
        structuredReport: {
            serializedValue: JSON.stringify(structuredReportData),
        },
    } as Partial<ExecutionRequestResult>;
};

<<<<<<< HEAD
const EMPTY_AND_CONDITONS = {
    on: {
        types: 'dataset',
        conditions: {
            and: [],
        },
    },
    rules: [],
};
const EMPTY_OR_CONDITONS = {
    on: {
        types: 'dataset',
        conditions: {
            or: [],
        },
    },
    rules: [],
};
const EMPTY_NOT_CONDITONS = {
    on: {
        types: 'dataset',
        conditions: {
            not: [],
        },
    },
    rules: [],
};

const DEAFULT_EMPTY_CONDITIONS_RESULT = {
    on: {
        types: 'dataset',
    },
    rules: [],
};

const PROPERTY_CONDITIONS = {
    on: {
        types: [],
        conditions: {
            and: {
                property: 'test',
                operator: 'exists',
            },
        },
    },
    rules: [],
};

const NULL_CONDITIONS = {
    source: {
        type: 'snowflake',
        config: {
            profiling: {
                profile_table_size_limit: null,
            },
        },
    },
};

const COMPLEX_CONDITIONS = {
    on: {
        types: ['dataset', 'dashboard'],
        conditions: {
            and: [
                { property: 'urn', operator: 'equals', values: ['sample', 'cypress'] },
                {
                    or: [
                        { property: 'entityType', operator: 'exists' },
                        { and: [{ not: [{ property: 'dataPlatformInstance.platform', operator: 'exists' }] }] },
                    ],
                },
                {
                    not: [
                        {
                            and: [
                                {
                                    property: 'ownership.owners.owner',
                                    operator: 'contains_any',
                                    values: ['urn:li:corpGroup:bfoo', 'urn:li:corpGroup:jdoe'],
                                },
                            ],
                        },
                        {
                            or: [
                                {
                                    property: 'domains.domains',
                                    operator: 'equals',
                                    values: ['urn:li:domain:marketing'],
                                },
                            ],
                        },
                    ],
                },
            ],
        },
    },
    rules: [],
};
describe('removeEmptyArrays', () => {
    test('removes empty AND condition array from a nested object', () => {
        const result = removeEmptyArrays(EMPTY_AND_CONDITONS);
        expect(result).toEqual(DEAFULT_EMPTY_CONDITIONS_RESULT);
    });

    test('removes empty OR condition array from a nested object', () => {
        const result = removeEmptyArrays(EMPTY_OR_CONDITONS);
        expect(result).toEqual(DEAFULT_EMPTY_CONDITIONS_RESULT);
    });

    test('removes empty NOT COndition array from a nested object', () => {
        const result = removeEmptyArrays(EMPTY_NOT_CONDITONS);
        expect(result).toEqual(DEAFULT_EMPTY_CONDITIONS_RESULT);
    });

    test('should remain same as there is condition is present', () => {
        const result = removeEmptyArrays(PROPERTY_CONDITIONS);
        expect(result).toEqual(PROPERTY_CONDITIONS);
    });

    test('should remain same as conditions are complex but not empty', () => {
        const result = removeEmptyArrays(COMPLEX_CONDITIONS);
        expect(result).toEqual(COMPLEX_CONDITIONS);
    });

    test('preserves null values in config settings', () => {
        const result = removeEmptyArrays(NULL_CONDITIONS);
        expect(result).toEqual(NULL_CONDITIONS);
        expect(result.source.config.profiling.profile_table_size_limit).toBeNull();
    });
});

=======
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
describe('getEntitiesIngestedByType', () => {
    // Mock for console.error
    const originalConsoleError = console.error;
    console.error = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterAll(() => {
        console.error = originalConsoleError;
    });

    test('returns null when structured report is not available', () => {
        const result = getEntitiesIngestedByType({} as Partial<ExecutionRequestResult>);
        expect(result).toBeNull();
    });

    test('returns null when an exception occurs during processing', () => {
        // Create a malformed structured report to trigger an exception
        const malformedReport = {
            source: {
                report: {
                    // Missing aspects property to trigger exception
                },
            },
        };

        const result = getEntitiesIngestedByType(mockExecutionRequestResult(malformedReport));
        expect(result).toBeNull();
    });

    test('correctly extracts entity counts from structured report', () => {
        // Create a structured report based on the example in the comments
        const structuredReport = {
            source: {
                report: {
                    aspects: {
                        container: {
                            containerProperties: 156,
                            container: 117,
                        },
                        dataset: {
                            status: 1505,
                            schemaMetadata: 1505,
                            datasetProperties: 1505,
                            container: 1505,
                            operation: 1521,
                        },
                    },
                },
            },
        };

        const result = getEntitiesIngestedByType(mockExecutionRequestResult(structuredReport));

        expect(result).toEqual([
            {
                count: 156,
                displayName: 'container',
            },
            {
                count: 1521,
                displayName: 'dataset',
            },
        ]);
    });

    test('handles empty aspects object', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects: {},
                },
            },
        };

        const result = getEntitiesIngestedByType(mockExecutionRequestResult(structuredReport));
        expect(result).toEqual([]);
    });

    test('handles aspects with non-numeric values', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects: {
                        container: {
                            containerProperties: '156',
                            container: 117,
                        },
                    },
                },
            },
        };

        const result = getEntitiesIngestedByType(mockExecutionRequestResult(structuredReport));
        expect(result).toEqual([
            {
                count: 156,
                displayName: 'container',
            },
        ]);
    });
});
