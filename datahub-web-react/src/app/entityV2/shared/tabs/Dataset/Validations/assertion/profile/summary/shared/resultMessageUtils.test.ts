import { getFormattedActualVsExpectedTextForVolumeAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

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
});
