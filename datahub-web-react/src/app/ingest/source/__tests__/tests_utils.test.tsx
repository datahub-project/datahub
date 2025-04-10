import { vi, describe, test, expect, beforeEach, afterAll } from 'vitest';
import { getEntitiesIngestedByType } from '../utils';
import { ExecutionRequestResult } from '../../../../types.generated';


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
