import { AssertionExpectedRange } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultExtractionUtils';
import {
    getFormattedActualVsExpectedTextForVolumeAssertion,
    getFormattedExpectedResultTextForAbsoluteAssertionRange,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import {
    AssertionResultType,
    AssertionRunStatus,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    VolumeAssertionType,
} from '@types';

describe('getFormattedActualVsExpectedText', () => {
    const baseRun = {
        timestampMillis: 1234567890,
        asserteeUrn: 'test-urn',
        assertionUrn: 'test-assertion-urn',
        runId: 'test-run-id',
        status: AssertionRunStatus.Complete,
        __typename: 'AssertionRunEvent' as const,
    };

    const baseRowCountTotal = {
        operator: AssertionStdOperator.Between,
        parameters: {
            minValue: { value: '100', type: AssertionStdParameterType.Number },
            maxValue: { value: '200', type: AssertionStdParameterType.Number },
        },
    };

    it('should return undefined for non-volume assertions', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Freshness,
                },
            },
        };
        expect(getFormattedActualVsExpectedTextForVolumeAssertion(run)).toBeUndefined();
    });

    it('should return undefined for RowCountChange type', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountChange,
                        entityUrn: 'test-entity-urn',
                    },
                },
            },
        };
        expect(getFormattedActualVsExpectedTextForVolumeAssertion(run)).toBeUndefined();
    });

    it('should handle error state', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Error,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            ...baseRowCountTotal,
                        },
                    },
                },
                rowCount: 150,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '',
            expectedLowText: '100',
            expectedHighText: '200',
            expectedLowTextWithDecimals: '100',
            expectedHighTextWithDecimals: '200',
        });
    });

    it('should format actual and expected values correctly for RowCountTotal', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            ...baseRowCountTotal,
                        },
                    },
                },
                rowCount: 150,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '150',
            expectedLowText: '100',
            expectedHighText: '200',
            expectedLowTextWithDecimals: '100',
            expectedHighTextWithDecimals: '200',
        });
    });

    it('should handle decimal values correctly', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            operator: AssertionStdOperator.Between,
                            parameters: {
                                minValue: { value: '100.123', type: AssertionStdParameterType.Number },
                                maxValue: { value: '200.456', type: AssertionStdParameterType.Number },
                            },
                        },
                    },
                },
                rowCount: 150.789,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '151',
            expectedLowText: '100',
            expectedHighText: '200',
            expectedLowTextWithDecimals: '100.1',
            expectedHighTextWithDecimals: '200.5',
        });
    });

    it('should handle missing range values', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            operator: AssertionStdOperator.Between,
                            parameters: {},
                        },
                    },
                },
                rowCount: 150,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '150',
            expectedLowText: '',
            expectedHighText: '',
            expectedLowTextWithDecimals: '',
            expectedHighTextWithDecimals: '',
        });
    });

    it('should handle undefined volume assertion', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                },
            },
        };
        expect(getFormattedActualVsExpectedTextForVolumeAssertion(run)).toBeUndefined();
    });

    it('should handle one-sided ranges correctly', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            operator: AssertionStdOperator.LessThanOrEqualTo,
                            parameters: {
                                value: { value: '200', type: AssertionStdParameterType.Number },
                            },
                        },
                    },
                },
                rowCount: 150,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '150',
            expectedLowText: '',
            expectedHighText: '200 or less',
            expectedLowTextWithDecimals: '',
            expectedHighTextWithDecimals: '200 or less',
        });
    });

    it('should handle exclusive one-sided ranges correctly', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            operator: AssertionStdOperator.LessThan,
                            parameters: {
                                value: { value: '200', type: AssertionStdParameterType.Number },
                            },
                            context: {
                                highType: 'exclusive',
                            },
                        },
                    },
                },
                rowCount: 150,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '150',
            expectedLowText: '',
            expectedHighText: 'Less than 200',
            expectedLowTextWithDecimals: '',
            expectedHighTextWithDecimals: 'Less than 200',
        });
    });

    it('should handle EqualTo operator correctly', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Success,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            operator: AssertionStdOperator.EqualTo,
                            parameters: {
                                value: { value: '150', type: AssertionStdParameterType.Number },
                            },
                        },
                    },
                },
                rowCount: 150,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '150',
            expectedLowText: '',
            expectedHighText: '',
            expectedLowTextWithDecimals: '',
            expectedHighTextWithDecimals: '',
        });
    });

    it('should handle NotEqualTo operator correctly', () => {
        const run = {
            ...baseRun,
            result: {
                type: AssertionResultType.Failure,
                assertion: {
                    type: AssertionType.Volume,
                    volumeAssertion: {
                        type: VolumeAssertionType.RowCountTotal,
                        entityUrn: 'test-entity-urn',
                        rowCountTotal: {
                            operator: AssertionStdOperator.NotEqualTo,
                            parameters: {
                                value: { value: '100', type: AssertionStdParameterType.Number },
                            },
                        },
                    },
                },
                rowCount: 100,
            },
        };
        const result = getFormattedActualVsExpectedTextForVolumeAssertion(run);
        expect(result).toEqual({
            actualText: '100',
            expectedLowText: '',
            expectedHighText: '',
            expectedLowTextWithDecimals: '',
            expectedHighTextWithDecimals: '',
        });
    });
});

describe('getFormattedExpectedResultTextForAbsoluteAssertionRange', () => {
    it('should format range with both high and low values', () => {
        const range: AssertionExpectedRange = {
            low: 100,
            high: 200,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be between 100 and 200.');
    });

    it('should format range with only high value (inclusive)', () => {
        const range: AssertionExpectedRange = {
            high: 200,
            context: { highType: 'inclusive' },
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be less than or equal to 200.');
    });

    it('should format range with only high value (exclusive)', () => {
        const range: AssertionExpectedRange = {
            high: 200,
            context: { highType: 'exclusive' },
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be less than 200.');
    });

    it('should format range with only low value (inclusive)', () => {
        const range: AssertionExpectedRange = {
            low: 100,
            context: { lowType: 'inclusive' },
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be greater than or equal to 100.');
    });

    it('should format range with only low value (exclusive)', () => {
        const range: AssertionExpectedRange = {
            low: 100,
            context: { lowType: 'exclusive' },
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be greater than 100.');
    });

    it('should format equal value', () => {
        const range: AssertionExpectedRange = {
            equal: 150,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be 150.');
    });

    it('should format not equal value', () => {
        const range: AssertionExpectedRange = {
            notEqual: 0,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should not be 0.');
    });

    it('should handle decimal values', () => {
        const range: AssertionExpectedRange = {
            low: 100.5,
            high: 200.7,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Metric value', range);
        expect(result).toBe('Metric value should be between 100.5 and 200.7.');
    });

    it('should handle high value without context (defaults to exclusive)', () => {
        const range: AssertionExpectedRange = {
            high: 200,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be less than 200.');
    });

    it('should handle low value without context (defaults to exclusive)', () => {
        const range: AssertionExpectedRange = {
            low: 100,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be greater than 100.');
    });

    it('should return undefined for empty range', () => {
        const range: AssertionExpectedRange = {};
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBeUndefined();
    });

    it('should handle custom description text', () => {
        const range: AssertionExpectedRange = {
            equal: 42,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Column value', range);
        expect(result).toBe('Column value should be 42.');
    });

    it('should handle zero values correctly', () => {
        const range: AssertionExpectedRange = {
            low: 0,
            high: 0,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be between 0 and 0.');
    });

    it('should handle negative values correctly', () => {
        const range: AssertionExpectedRange = {
            low: -100,
            high: -50,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Change value', range);
        expect(result).toBe('Change value should be between -100 and -50.');
    });

    it('should prioritize both high and low when present with other values', () => {
        const range: AssertionExpectedRange = {
            low: 100,
            high: 200,
            equal: 150,
            notEqual: 0,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be between 100 and 200.');
    });

    it('should prioritize high when both high and equal are present', () => {
        const range: AssertionExpectedRange = {
            high: 200,
            equal: 150,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be less than 200.');
    });

    it('should prioritize low when both low and equal are present', () => {
        const range: AssertionExpectedRange = {
            low: 100,
            equal: 150,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be greater than 100.');
    });

    it('should prioritize equal when both equal and notEqual are present', () => {
        const range: AssertionExpectedRange = {
            equal: 150,
            notEqual: 0,
        };
        const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
        expect(result).toBe('Row count should be 150.');
    });
});
