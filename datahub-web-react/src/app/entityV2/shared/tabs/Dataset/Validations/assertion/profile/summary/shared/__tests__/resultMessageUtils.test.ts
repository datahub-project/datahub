import { AssertionExpectedRange } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultExtractionUtils';
import {
    getFormattedActualVsExpectedTextForVolumeAssertion,
    getFormattedExpectedResultTextForAbsoluteAssertionRange,
    getFormattedReasonText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import {
    AssertionResultType,
    AssertionRunStatus,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    EntityType,
    FieldAssertionType,
    FieldMetricType,
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

    describe('metricType parameter', () => {
        it('should format percentage values correctly with metricType percentage', () => {
            const range: AssertionExpectedRange = {
                low: 10.5,
                high: 25.75,
            };
            const result = getFormattedExpectedResultTextForAbsoluteAssertionRange(
                'Null percentage',
                range,
                'percentage',
            );
            expect(result).toBe('Null percentage should be between 10.5% and 25.75%.');
        });

        it('should format single percentage value correctly', () => {
            const range: AssertionExpectedRange = {
                equal: 15.25,
            };
            const result = getFormattedExpectedResultTextForAbsoluteAssertionRange(
                'Empty percentage',
                range,
                'percentage',
            );
            expect(result).toBe('Empty percentage should be 15.25%.');
        });

        it('should format percentage range with only high value', () => {
            const range: AssertionExpectedRange = {
                high: 50.0,
                context: { highType: 'inclusive' },
            };
            const result = getFormattedExpectedResultTextForAbsoluteAssertionRange(
                'Null percentage',
                range,
                'percentage',
            );
            expect(result).toBe('Null percentage should be less than or equal to 50%.');
        });

        it('should format percentage range with only low value', () => {
            const range: AssertionExpectedRange = {
                low: 5.5,
                context: { lowType: 'exclusive' },
            };
            const result = getFormattedExpectedResultTextForAbsoluteAssertionRange(
                'Unique percentage',
                range,
                'percentage',
            );
            expect(result).toBe('Unique percentage should be greater than 5.5%.');
        });

        it('should default to absolute formatting when metricType is not specified', () => {
            const range: AssertionExpectedRange = {
                equal: 1000,
            };
            const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range);
            expect(result).toBe('Row count should be 1,000.');
        });

        it('should format absolute values correctly with metricType absolute', () => {
            const range: AssertionExpectedRange = {
                low: 100,
                high: 200,
            };
            const result = getFormattedExpectedResultTextForAbsoluteAssertionRange('Row count', range, 'absolute');
            expect(result).toBe('Row count should be between 100 and 200.');
        });
    });
});

describe('getFormattedReasonText for Field Metric Assertions', () => {
    const baseAssertion = {
        urn: 'test-assertion-urn',
        type: EntityType.Assertion,
        info: {
            type: AssertionType.Field,
            fieldAssertion: {
                type: FieldAssertionType.FieldMetric,
                fieldMetricAssertion: {
                    field: {
                        path: 'age_column',
                    },
                    metric: FieldMetricType.NullPercentage,
                },
            },
        },
    } as any; // Using any to simplify test setup

    const baseRun = {
        timestampMillis: 1234567890,
        asserteeUrn: 'test-urn',
        assertionUrn: 'test-assertion-urn',
        runId: 'test-run-id',
        status: AssertionRunStatus.Complete,
        __typename: 'AssertionRunEvent' as const,
    } as any; // Using any to simplify test setup

    describe('percentage metrics', () => {
        it('should format percentage metric with actual value - success case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Success,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'age_column',
                                },
                                metric: FieldMetricType.NullPercentage,
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '5' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Null percentage of age_column (5%) met the expected conditions.');
        });

        it('should format percentage metric with actual value - failure case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Failure,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'email_field',
                                },
                                metric: FieldMetricType.EmptyPercentage,
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '15.5' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Empty percentage of email_field (15.5%) did not meet the expected conditions.');
        });

        it('should format percentage metric without actual value - success case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Success,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'status_column',
                                },
                                metric: FieldMetricType.UniquePercentage,
                            },
                        },
                    },
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Unique percentage of status_column met the expected conditions.');
        });

        it('should format percentage metric without actual value - failure case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Failure,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'score_field',
                                },
                                metric: FieldMetricType.ZeroPercentage,
                            },
                        },
                    },
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Zero percentage of score_field did not meet the expected conditions.');
        });
    });

    describe('non-percentage metrics', () => {
        it('should format non-percentage metric with actual value - success case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Success,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'user_id',
                                },
                                metric: FieldMetricType.UniqueCount,
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '1000' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Unique count of user_id (1000) met the expected conditions.');
        });

        it('should format non-percentage metric with actual value - failure case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Failure,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'price',
                                },
                                metric: FieldMetricType.Max,
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '9999.99' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Max of price (9999.99) did not meet the expected conditions.');
        });

        it('should format non-percentage metric without actual value - success case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Success,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'amount',
                                },
                                metric: FieldMetricType.Mean,
                            },
                        },
                    },
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Average of amount met the expected conditions.');
        });

        it('should format non-percentage metric without actual value - failure case', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Failure,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'description',
                                },
                                metric: FieldMetricType.MinLength,
                            },
                        },
                    },
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Min length of description did not meet the expected conditions.');
        });
    });

    describe('edge cases', () => {
        it('should handle missing field path', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Success,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                metric: FieldMetricType.NullPercentage,
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '10' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Null percentage of column (10%) met the expected conditions.');
        });

        it('should handle missing metric type', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Failure,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'test_field',
                                },
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '42' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Aggregation of test_field (42) did not meet the expected conditions.');
        });

        it('should handle zero actual value for percentage metric', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Success,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'error_count',
                                },
                                metric: FieldMetricType.NegativePercentage,
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '0' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Negative percentage of error_count (0%) met the expected conditions.');
        });

        it('should handle zero actual value for non-percentage metric', () => {
            const run = {
                ...baseRun,
                result: {
                    type: AssertionResultType.Success,
                    assertion: {
                        type: AssertionType.Field,
                        fieldAssertion: {
                            type: FieldAssertionType.FieldMetric,
                            fieldMetricAssertion: {
                                field: {
                                    path: 'null_count',
                                },
                                metric: FieldMetricType.NullCount,
                            },
                        },
                    },
                    nativeResults: [{ key: 'Metric Value', value: '0' }],
                },
            };

            const result = getFormattedReasonText(baseAssertion, run);
            expect(result).toBe('Null count of null_count (0) met the expected conditions.');
        });
    });
});
