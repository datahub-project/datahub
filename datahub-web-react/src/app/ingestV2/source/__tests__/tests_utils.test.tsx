import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import { afterAll, beforeEach, describe, expect, test, vi } from 'vitest';

import { SortingState } from '@components/components/Table/types';

import {
    EXECUTION_REQUEST_STATUS_LOADING,
    EXECUTION_REQUEST_STATUS_PENDING,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import {
    buildOwnerEntities,
    capitalizeMonthsAndDays,
    formatTimezone,
    getEntitiesIngestedByType,
    getSortInput,
    getSourceStatus,
    getTotalEntitiesIngested,
} from '@app/ingestV2/source/utils';

import { EntityType, ExecutionRequest, ExecutionRequestResult, IngestionSource, SortOrder } from '@types';

// Extend dayjs with required plugins
dayjs.extend(utc);
dayjs.extend(timezone);

// Mock the structuredReport property of ExecutionRequestResult
const mockExecutionRequestResult = (structuredReportData: any): Partial<ExecutionRequestResult> => {
    return {
        structuredReport: {
            serializedValue: JSON.stringify(structuredReportData),
        },
    } as Partial<ExecutionRequestResult>;
};

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
                count: 1505,
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
        expect(result).toBeNull();
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

describe('getSortInput', () => {
    it('returns undefined for original sorting', () => {
        expect(getSortInput('name', SortingState.ORIGINAL)).toBeUndefined();
    });

    it('returns ascending sort input', () => {
        expect(getSortInput('name', SortingState.ASCENDING)).toEqual({
            sortOrder: SortOrder.Ascending,
            field: 'name',
        });
    });

    it('returns descending sort input', () => {
        expect(getSortInput('name', SortingState.DESCENDING)).toEqual({
            sortOrder: SortOrder.Descending,
            field: 'name',
        });
    });
});

describe('getTotalEntitiesIngested', () => {
    test('returns null when structured report is not available', () => {
        const result = getTotalEntitiesIngested({} as Partial<ExecutionRequestResult>);
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

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(malformedReport));
        expect(result).toBeNull();
    });

    test('returns null when aspects object is empty', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects: {},
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport));
        expect(result).toBeNull();
    });

    test('correctly calculates total from multiple entity types', () => {
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
                        dashboard: {
                            status: 42,
                            dashboardInfo: 42,
                        },
                    },
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport));
        expect(result).toBe(156 + 1505 + 42); // 1703
    });

    test('correctly calculates total from single entity type', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects: {
                        container: {
                            containerProperties: 156,
                            container: 117,
                        },
                    },
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport));
        expect(result).toBe(156);
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

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport));
        expect(result).toBe(156);
    });
});

describe('formatTimezone', () => {
    it('should return undefined for null input', () => {
        expect(formatTimezone(null)).toBeUndefined();
    });

    it('should return undefined for undefined input', () => {
        expect(formatTimezone(undefined)).toBeUndefined();
    });

    it('should format valid timezone correctly', () => {
        // Mock the current time to ensure consistent testing
        const mockDate = new Date('2024-01-01T12:00:00Z');
        vi.spyOn(global.Date, 'now').mockImplementation(() => mockDate.getTime());

        // Test timezone abbreviations that can vary based on DST
        const nycAbbr = formatTimezone('America/New_York');
        expect(['EST', 'EDT']).toContain(nycAbbr);

        const londonAbbr = formatTimezone('Europe/London');
        expect(['GMT+1', 'BST']).toContain(londonAbbr);

        // Tokyo doesn't observe DST, so it's always GMT+9
        expect(formatTimezone('Asia/Tokyo')).toBe('GMT+9');

        // Clean up
        vi.restoreAllMocks();
    });

    it('should handle invalid timezone gracefully', () => {
        // Mock the current time to ensure consistent testing
        const mockDate = new Date('2024-01-01T12:00:00Z');
        vi.spyOn(global.Date, 'now').mockImplementation(() => mockDate.getTime());

        // Invalid timezone should return undefined or throw an error
        expect(() => formatTimezone('Invalid/Timezone')).toThrow();

        // Clean up
        vi.restoreAllMocks();
    });
});

describe('capitalizeMonthsAndDays', () => {
    it('should capitalize month names', () => {
        const input = 'january february march april may june july august september october november december';
        const expected = 'January February March April May June July August September October November December';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should capitalize day names', () => {
        const input = 'monday tuesday wednesday thursday friday saturday sunday';
        const expected = 'Monday Tuesday Wednesday Thursday Friday Saturday Sunday';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should handle mixed case input', () => {
        const input = 'monday January tuesday February';
        const expected = 'Monday January Tuesday February';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should not capitalize non-month/day words', () => {
        const input = 'hello world monday january test';
        const expected = 'hello world Monday January test';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should handle empty string', () => {
        expect(capitalizeMonthsAndDays('')).toBe('');
    });

    it('should handle string with no month or day names', () => {
        const input = 'this is a test string';
        expect(capitalizeMonthsAndDays(input)).toBe(input);
    });

    it('should handle string with special characters', () => {
        const input = 'monday, january 1st - tuesday, february 2nd';
        const expected = 'Monday, January 1st - Tuesday, February 2nd';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });
});

describe('getSourceStatus', () => {
    const urn = 'urn:li:source:123';

    const createSource = (sourceUrn: string, requests: ExecutionRequest[] = []): IngestionSource => ({
        urn: sourceUrn,
        executions: {
            executionRequests: requests,
        },
        config: {
            executorId: 'executorId',
            recipe: '',
        },
        name: 'source',
        type: 'snowflake',
    });

    const createExecutionRequest = (overrides: Partial<ExecutionRequest> = {}): ExecutionRequest => ({
        result: { status: EXECUTION_REQUEST_STATUS_SUCCESS },
        id: 'request',
        urn: 'urn:li:request',
        type: EntityType.ExecutionRequest,
        input: {
            requestedAt: 0,
            source: {
                type: 'INGESTION_SOURCE',
            },
            task: '',
        },
        ...overrides,
    });

    it('returns Pending when polling and no requests but did not execute', () => {
        const source = createSource(urn, []);
        const result = getSourceStatus(source, new Set([urn]), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_PENDING);
    });

    it('returns previous status when polling with no active request but did not execute', () => {
        const inactiveRequest = createExecutionRequest({ result: { status: EXECUTION_REQUEST_STATUS_SUCCESS } });
        const source = createSource(urn, [inactiveRequest]);
        const result = getSourceStatus(source, new Set([urn]), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_SUCCESS);
    });

    it('returns Loading when recently executed but no active requests', () => {
        const source = createSource(urn, []);
        const result = getSourceStatus(source, new Set(), new Set([urn]));
        expect(result).toBe(EXECUTION_REQUEST_STATUS_LOADING);
    });

    it('returns Success from the last request when not polling', () => {
        const source = createSource(urn, [
            createExecutionRequest({ result: { status: EXECUTION_REQUEST_STATUS_SUCCESS } }),
        ]);
        const result = getSourceStatus(source, new Set(), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_SUCCESS);
    });

    it('returns Pending when not polling, not executed, and no requests', () => {
        const source = createSource(urn);
        const result = getSourceStatus(source, new Set(), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_PENDING);
    });

    it('returns Pending when not polling, not executed and no request result,', () => {
        const source = createSource(urn, [createExecutionRequest({ result: undefined })]);
        const result = getSourceStatus(source, new Set(), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_PENDING);
    });
});

describe('buildOwnerEntities', () => {
    const entityUrn = 'urn:li:entity:123';
    const ownerUrn = 'urn:li:user:123';
    const defaultOwnerType = { urn: 'urn:li:ownershipType:custom', type: EntityType.CustomOwnershipType };

    it('should return an empty array when owners is undefined', () => {
        expect(buildOwnerEntities(entityUrn, undefined, defaultOwnerType)).toEqual([]);
    });

    it('should return an empty array when owners is empty', () => {
        expect(buildOwnerEntities(entityUrn, [], defaultOwnerType)).toEqual([]);
    });

    it('should apply all defaults when owner fields are missing', () => {
        const owners = [{ type: EntityType.CorpUser, urn: ownerUrn }];
        const result = buildOwnerEntities(entityUrn, owners, defaultOwnerType);

        expect(result).toEqual([
            {
                owner: {
                    type: 'CORP_USER',
                    urn: ownerUrn,
                    editableProperties: {
                        email: '',
                        displayName: '',
                        title: '',
                        pictureLink: '',
                    },
                    properties: {
                        displayName: '',
                        email: '',
                        active: true,
                        firstName: '',
                        lastName: '',
                        fullName: '',
                        title: '',
                    },
                    info: {
                        email: '',
                        admins: [],
                        members: [],
                        groups: [],
                        active: true,
                        displayName: '',
                        firstName: '',
                        lastName: '',
                        fullName: '',
                        title: '',
                    },
                },
                associatedUrn: entityUrn,
                type: 'CORP_USER',
                ownershipType: defaultOwnerType,
                __typename: 'Owner',
            },
        ]);
    });

    it('should override defaults with owner values', () => {
        const owners = [
            {
                type: EntityType.CorpUser,
                urn: ownerUrn,
                editableProperties: {
                    email: 'test@example.com',
                    displayName: 'Test User',
                },
                properties: {
                    displayName: 'Test User',
                    email: 'test@example.com',
                    active: false,
                },
                info: {
                    email: 'test@example.com',
                    active: false,
                    admins: ['admin1'],
                },
            },
        ];
        const result = buildOwnerEntities(entityUrn, owners, defaultOwnerType);

        expect(result[0].owner.editableProperties.email).toBe('test@example.com');
        expect(result[0].owner.editableProperties.displayName).toBe('Test User');
        expect(result[0].owner.properties.active).toBe(false);
        expect(result[0].owner.info.active).toBe(false);
        expect(result[0].owner.info.admins).toEqual(['admin1']);
    });

    it('should set ownershipType to null if not provided', () => {
        const owners = [{ type: EntityType.CorpUser, urn: ownerUrn }];
        const result = buildOwnerEntities(entityUrn, owners, undefined);
        expect(result[0].ownershipType).toBeNull();
    });

    it('should handle partial owner objects', () => {
        const owners = [
            { type: EntityType.CorpGroup, urn: ownerUrn, editableProperties: { displayName: 'Partial User' } },
        ];
        const result = buildOwnerEntities(entityUrn, owners, defaultOwnerType);
        expect(result[0].owner.editableProperties.displayName).toBe('Partial User');
        expect(result[0].owner.properties.displayName).toBe('');
        expect(result[0].owner.info.admins).toEqual([]);
    });
});
