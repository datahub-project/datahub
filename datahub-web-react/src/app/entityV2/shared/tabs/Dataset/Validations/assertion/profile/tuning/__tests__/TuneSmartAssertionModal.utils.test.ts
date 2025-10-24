import { transformData } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuneSmartAssertionModal.utils';

import { ListMonitorMetricsQuery } from '@graphql/monitor.generated';
import { AnomalyReviewState, AnomalySourceType } from '@types';

describe('TuneSmartAssertionModal.utils', () => {
    describe('transformData', () => {
        it('should return empty array when data is undefined', () => {
            const result = transformData(undefined);
            expect(result).toEqual([]);
        });

        it('should return empty array when listMonitorMetrics is undefined', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [],
                },
            };
            const result = transformData(data);
            expect(result).toEqual([]);
        });

        it('should return empty array when metrics array is empty', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [],
                },
            };
            const result = transformData(data);
            expect(result).toEqual([]);
        });

        it('should transform valid metrics data correctly', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000, // 2022-01-01 00:00:00 UTC
                                value: 100,
                            },
                            anomalyEvent: null,
                        },
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1641081600000, // 2022-01-02 00:00:00 UTC
                                value: 150,
                            },
                            anomalyEvent: null,
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 100,
                    hasAnomaly: false,
                },
                {
                    x: '2022-01-02T00:00:00.000Z',
                    y: 150,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should handle metrics with anomalies correctly', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: 100,
                            },
                            anomalyEvent: {
                                __typename: 'MonitorAnomalyEvent',
                                state: AnomalyReviewState.Confirmed,
                                timestampMillis: 1640995200000,
                                source: {
                                    __typename: 'AnomalySource',
                                    type: AnomalySourceType.InferredAssertionFailure,
                                    sourceUrn: 'urn:li:assertion:test',
                                    sourceEventTimestampMillis: 1640995200000,
                                    properties: {
                                        __typename: 'AnomalySourceProperties',
                                        assertionMetric: {
                                            __typename: 'AssertionMetric',
                                            timestampMillis: 1640995200000,
                                            value: 100,
                                        },
                                    },
                                },
                            },
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 100,
                    hasAnomaly: true,
                },
            ]);
        });

        it('should handle rejected anomalies correctly (should not mark as anomaly)', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: 100,
                            },
                            anomalyEvent: {
                                __typename: 'MonitorAnomalyEvent',
                                state: AnomalyReviewState.Rejected,
                                timestampMillis: 1640995200000,
                                source: {
                                    __typename: 'AnomalySource',
                                    type: AnomalySourceType.InferredAssertionFailure,
                                    sourceUrn: 'urn:li:assertion:test',
                                    sourceEventTimestampMillis: 1640995200000,
                                    properties: {
                                        __typename: 'AnomalySourceProperties',
                                        assertionMetric: {
                                            __typename: 'AssertionMetric',
                                            timestampMillis: 1640995200000,
                                            value: 100,
                                        },
                                    },
                                },
                            },
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 100,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should filter out metrics with missing timestampMillis', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: null as any,
                                value: 100,
                            },
                            anomalyEvent: null,
                        },
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: 150,
                            },
                            anomalyEvent: null,
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 150,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should filter out metrics with null value', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: null as any,
                            },
                            anomalyEvent: null,
                        },
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1641081600000,
                                value: 150,
                            },
                            anomalyEvent: null,
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-02T00:00:00.000Z',
                    y: 150,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should handle metrics with missing assertionMetric', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: null as any,
                                value: null as any,
                            },
                            anomalyEvent: null,
                        },
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: 150,
                            },
                            anomalyEvent: null,
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 150,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should sort metrics by timestamp in ascending order', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1641081600000, // 2022-01-02
                                value: 200,
                            },
                            anomalyEvent: null,
                        },
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000, // 2022-01-01
                                value: 100,
                            },
                            anomalyEvent: null,
                        },
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1641168000000, // 2022-01-03
                                value: 300,
                            },
                            anomalyEvent: null,
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 100,
                    hasAnomaly: false,
                },
                {
                    x: '2022-01-02T00:00:00.000Z',
                    y: 200,
                    hasAnomaly: false,
                },
                {
                    x: '2022-01-03T00:00:00.000Z',
                    y: 300,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should handle zero values correctly', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: 0,
                            },
                            anomalyEvent: null,
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 0,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should handle negative values correctly', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: -50,
                            },
                            anomalyEvent: null,
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: -50,
                    hasAnomaly: false,
                },
            ]);
        });

        it('should handle mixed scenarios with valid and invalid data', () => {
            const data: ListMonitorMetricsQuery = {
                listMonitorMetrics: {
                    __typename: 'ListMonitorMetricsResult',
                    metrics: [
                        // Invalid - no timestamp
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: null as any,
                                value: 100,
                            },
                            anomalyEvent: null,
                        },
                        // Valid with confirmed anomaly
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1640995200000,
                                value: 150,
                            },
                            anomalyEvent: {
                                __typename: 'MonitorAnomalyEvent',
                                state: AnomalyReviewState.Confirmed,
                                timestampMillis: 1640995200000,
                                source: {
                                    __typename: 'AnomalySource',
                                    type: AnomalySourceType.InferredAssertionFailure,
                                    sourceUrn: 'urn:li:assertion:test',
                                    sourceEventTimestampMillis: 1640995200000,
                                    properties: {
                                        __typename: 'AnomalySourceProperties',
                                        assertionMetric: {
                                            __typename: 'AssertionMetric',
                                            timestampMillis: 1640995200000,
                                            value: 150,
                                        },
                                    },
                                },
                            },
                        },
                        // Invalid - null value
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1641081600000,
                                value: null as any,
                            },
                            anomalyEvent: null,
                        },
                        // Valid with rejected anomaly
                        {
                            __typename: 'MonitorMetric',
                            assertionMetric: {
                                __typename: 'AssertionMetric',
                                timestampMillis: 1641168000000,
                                value: 200,
                            },
                            anomalyEvent: {
                                __typename: 'MonitorAnomalyEvent',
                                state: AnomalyReviewState.Rejected,
                                timestampMillis: 1641168000000,
                                source: {
                                    __typename: 'AnomalySource',
                                    type: AnomalySourceType.InferredAssertionFailure,
                                    sourceUrn: 'urn:li:assertion:test',
                                    sourceEventTimestampMillis: 1641168000000,
                                    properties: {
                                        __typename: 'AnomalySourceProperties',
                                        assertionMetric: {
                                            __typename: 'AssertionMetric',
                                            timestampMillis: 1641168000000,
                                            value: 200,
                                        },
                                    },
                                },
                            },
                        },
                    ],
                },
            };

            const result = transformData(data);

            expect(result).toEqual([
                {
                    x: '2022-01-01T00:00:00.000Z',
                    y: 150,
                    hasAnomaly: true,
                },
                {
                    x: '2022-01-03T00:00:00.000Z',
                    y: 200,
                    hasAnomaly: false,
                },
            ]);
        });
    });
});
