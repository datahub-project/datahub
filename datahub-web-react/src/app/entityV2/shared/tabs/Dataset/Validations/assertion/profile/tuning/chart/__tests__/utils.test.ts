import moment from 'moment';
import { describe, expect, it } from 'vitest';

import {
    ANOMALY_COLOR,
    EXCLUSION_WINDOW_COLOR,
    METRICS_LINE_COLOR,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/constants';
import {
    MetricDataPoint,
    calculatePredictionOverlayPosition,
    calculatePredictionOverlayYPosition,
    calculateTooltipPosition,
    calculateVisibleExclusionWindows,
    calculateXAxisDomain,
    calculateYAxisDomain,
    createTimeScale,
    formatAxisDate,
    formatMetricNumber,
    formatTooltipDate,
    getExclusionWindowForTimestamp,
    getFillColor,
    getFuturePredictions,
    isTimestampInExclusionWindow,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/utils';

import { AssertionExclusionWindowType } from '@types';

// Mock data for tests
const mockMetricData: MetricDataPoint[] = [
    { x: '2024-01-01T00:00:00Z', y: 10, hasAnomaly: false },
    { x: '2024-01-02T00:00:00Z', y: 15, hasAnomaly: true },
    { x: '2024-01-03T00:00:00Z', y: 12, hasAnomaly: false },
];

const mockPredictions = [
    {
        index: 1,
        lowerBound: 5,
        upperBound: 20,
        timeWindow: {
            startTimeMillis: Date.now() + 86400000, // 1 day in future
            endTimeMillis: Date.now() + 172800000, // 2 days in future
        },
    },
    {
        index: 2,
        lowerBound: 8,
        upperBound: 25,
        timeWindow: {
            startTimeMillis: Date.now() - 86400000, // 1 day in past
            endTimeMillis: Date.now() - 43200000, // 12 hours in past
        },
    },
];

const mockExclusionWindows = [
    {
        type: AssertionExclusionWindowType.FixedRange,
        displayName: 'Test Window',
        fixedRange: {
            startTimeMillis: new Date('2024-01-01T12:00:00Z').getTime(),
            endTimeMillis: new Date('2024-01-01T18:00:00Z').getTime(),
        },
    },
];

describe('getFuturePredictions', () => {
    it('should filter out past predictions and return only future ones', () => {
        const result = getFuturePredictions(mockPredictions);
        expect(result).toHaveLength(1);
        expect(result[0].index).toBe(1);
    });

    it('should return empty array when no predictions provided', () => {
        const result = getFuturePredictions([]);
        expect(result).toEqual([]);
    });

    it('should filter out predictions without timeWindow', () => {
        const predictionsWithoutTime = [
            { index: 1, lowerBound: 5, upperBound: 20 },
            { index: 2, lowerBound: 8, upperBound: 25, timeWindow: {} },
        ];
        const result = getFuturePredictions(predictionsWithoutTime);
        expect(result).toEqual([]);
    });

    it('should sort future predictions by start time', () => {
        const futurePredictions = [
            {
                index: 2,
                timeWindow: { startTimeMillis: Date.now() + 172800000 }, // 2 days future
            },
            {
                index: 1,
                timeWindow: { startTimeMillis: Date.now() + 86400000 }, // 1 day future
            },
        ];
        const result = getFuturePredictions(futurePredictions);
        expect(result[0].index).toBe(1);
        expect(result[1].index).toBe(2);
    });
});

describe('calculateXAxisDomain', () => {
    it('should use date range when provided', () => {
        const startDate = moment('2024-01-01');
        const endDate = moment('2024-01-31');
        const dateRange: [moment.Moment, moment.Moment] = [startDate, endDate];

        const result = calculateXAxisDomain(dateRange, mockMetricData, []);
        expect(result[0]).toEqual(startDate.toDate());
        expect(result[1]).toEqual(endDate.toDate());
    });

    it('should use data range when no date range provided', () => {
        const dateRange: [null, null] = [null, null];
        const result = calculateXAxisDomain(dateRange, mockMetricData, []);

        expect(result[0]).toEqual(new Date('2024-01-01T00:00:00Z'));
        expect(result[1]).toEqual(new Date('2024-01-03T00:00:00Z'));
    });

    it('should extend domain to include future predictions', () => {
        const dateRange: [null, null] = [null, null];
        const futurePrediction = {
            index: 1,
            timeWindow: {
                startTimeMillis: new Date('2024-01-05T00:00:00Z').getTime(),
                endTimeMillis: new Date('2024-01-10T00:00:00Z').getTime(),
            },
        };

        const result = calculateXAxisDomain(dateRange, mockMetricData, [futurePrediction]);
        expect(result[1]).toEqual(new Date('2024-01-10T00:00:00Z'));
    });

    it('should return default dates when no data available', () => {
        const dateRange: [null, null] = [null, null];
        const result = calculateXAxisDomain(dateRange, [], []);
        expect(result[0]).toBeInstanceOf(Date);
        expect(result[1]).toBeInstanceOf(Date);
    });
});

describe('createTimeScale', () => {
    it('should create time scale with correct domain and range', () => {
        const domain: [Date, Date] = [new Date('2024-01-01'), new Date('2024-01-31')];
        const width = 800;
        const margin = { left: 50, right: 50 };

        const scale = createTimeScale(domain, width, margin);
        expect(scale.domain()).toEqual([domain[0], domain[1]]);
        expect(scale.range()).toEqual([margin.left, width - margin.right]);
    });
});

describe('calculateTooltipPosition', () => {
    it('should calculate tooltip position correctly', () => {
        const predictionRect = { left: 100, width: 50 } as DOMRect;
        const chartRect = { left: 0, width: 800 } as DOMRect;
        const height = 400;
        const margin = { top: 20, bottom: 20 };

        const result = calculateTooltipPosition(predictionRect, chartRect, height, margin);
        expect(result.x).toBe(25); // centered but adjusted for tooltip width
        expect(result.y).toBe(200); // center of chart area
    });

    it('should adjust position when tooltip would go off right edge', () => {
        const predictionRect = { left: 750, width: 50 } as DOMRect;
        const chartRect = { left: 0, width: 800 } as DOMRect;
        const height = 400;
        const margin = { top: 20, bottom: 20 };

        const result = calculateTooltipPosition(predictionRect, chartRect, height, margin);
        expect(result.x).toBe(580); // adjusted to stay within bounds
    });

    it('should adjust position when tooltip would go off left edge', () => {
        const predictionRect = { left: 10, width: 50 } as DOMRect;
        const chartRect = { left: 0, width: 800 } as DOMRect;
        const height = 400;
        const margin = { top: 20, bottom: 20 };

        const result = calculateTooltipPosition(predictionRect, chartRect, height, margin);
        expect(result.x).toBe(20); // adjusted to stay within bounds
    });
});

describe('formatting functions', () => {
    describe('formatTooltipDate', () => {
        it('should format timestamp correctly', () => {
            const timestamp = new Date('2024-01-15T14:30:00Z').getTime();
            const result = formatTooltipDate(timestamp);
            expect(result).toMatch(/Jan 15/);
            expect(result).toMatch(/30/); // Check for minute component
        });
    });

    describe('formatAxisDate', () => {
        it('should format value correctly', () => {
            const timestamp = new Date('2024-01-15T14:30:00Z').getTime();
            const result = formatAxisDate(timestamp);
            expect(result).toMatch(/Jan 15/);
            expect(result).toMatch(/30/); // Check for minute component
        });
    });

    describe('formatMetricNumber', () => {
        it('should format number with up to 4 decimal places removing trailing zeros', () => {
            const result = formatMetricNumber(1.123456789);
            expect(result).toBe('1.124'); // toLocaleString rounds further
        });

        it('should format large numbers with commas', () => {
            const result = formatMetricNumber(1000.5678);
            expect(result).toBe('1,000.568'); // toLocaleString rounds further
        });

        it('should handle numbers with fewer than 4 decimal places', () => {
            expect(formatMetricNumber(1.12)).toBe('1.12');
        });

        it('should handle integers', () => {
            expect(formatMetricNumber(1000)).toBe('1,000');
        });

        it('should handle zero', () => {
            expect(formatMetricNumber(0)).toBe('0');
        });

        it('should remove trailing zeros', () => {
            expect(formatMetricNumber(1.1)).toBe('1.1');
        });
    });
});

describe('exclusion window functions', () => {
    describe('isTimestampInExclusionWindow', () => {
        it('should return true when timestamp is in exclusion window', () => {
            const timestamp = new Date('2024-01-01T15:00:00Z').getTime();
            const result = isTimestampInExclusionWindow(timestamp, mockExclusionWindows);
            expect(result).toBe(true);
        });

        it('should return false when timestamp is not in exclusion window', () => {
            const timestamp = new Date('2024-01-01T10:00:00Z').getTime();
            const result = isTimestampInExclusionWindow(timestamp, mockExclusionWindows);
            expect(result).toBe(false);
        });

        it('should return false when no exclusion windows provided', () => {
            const timestamp = new Date('2024-01-01T15:00:00Z').getTime();
            const result = isTimestampInExclusionWindow(timestamp, []);
            expect(result).toBe(false);
        });
    });

    describe('getExclusionWindowForTimestamp', () => {
        it('should return exclusion window when timestamp matches', () => {
            const timestamp = new Date('2024-01-01T15:00:00Z').getTime();
            const result = getExclusionWindowForTimestamp(timestamp, mockExclusionWindows);
            expect(result).toEqual(mockExclusionWindows[0]);
        });

        it('should return null when timestamp does not match', () => {
            const timestamp = new Date('2024-01-01T10:00:00Z').getTime();
            const result = getExclusionWindowForTimestamp(timestamp, mockExclusionWindows);
            expect(result).toBe(null);
        });
    });
});

describe('calculateVisibleExclusionWindows', () => {
    it('should calculate visible exclusion windows correctly', () => {
        const domain: [Date, Date] = [new Date('2024-01-01T00:00:00Z'), new Date('2024-01-02T00:00:00Z')];

        const result = calculateVisibleExclusionWindows(mockExclusionWindows, domain);
        expect(result).toHaveLength(1);
        expect(result[0].leftPercent).toBe(50); // 12 hours into 24 hour period
        expect(result[0].widthPercent).toBe(25); // 6 hours duration
    });

    it('should filter out non-overlapping windows', () => {
        const domain: [Date, Date] = [new Date('2024-01-02T00:00:00Z'), new Date('2024-01-03T00:00:00Z')];

        const result = calculateVisibleExclusionWindows(mockExclusionWindows, domain);
        expect(result).toHaveLength(0);
    });
});

describe('calculatePredictionOverlayPosition', () => {
    it('should calculate position correctly when prediction is visible', () => {
        const startTime = new Date('2024-01-01T06:00:00Z').getTime();
        const endTime = new Date('2024-01-01T12:00:00Z').getTime();
        const domain: [Date, Date] = [new Date('2024-01-01T00:00:00Z'), new Date('2024-01-02T00:00:00Z')];

        const result = calculatePredictionOverlayPosition(startTime, endTime, domain);
        expect(result.isVisible).toBe(true);
        expect(result.leftPercent).toBe(25); // 6 hours into 24 hour period
        expect(result.widthPercent).toBe(25); // 6 hours duration
    });

    it('should return not visible when prediction is outside domain', () => {
        const startTime = new Date('2024-01-03T00:00:00Z').getTime();
        const endTime = new Date('2024-01-03T06:00:00Z').getTime();
        const domain: [Date, Date] = [new Date('2024-01-01T00:00:00Z'), new Date('2024-01-02T00:00:00Z')];

        const result = calculatePredictionOverlayPosition(startTime, endTime, domain);
        expect(result.isVisible).toBe(false);
        expect(result.leftPercent).toBe(0);
        expect(result.widthPercent).toBe(0);
    });
});

describe('calculatePredictionOverlayYPosition', () => {
    it('should calculate Y position with both bounds', () => {
        const prediction = { index: 1, lowerBound: 10, upperBound: 20 };
        const yDomain: [number, number] = [0, 30];

        const result = calculatePredictionOverlayYPosition(prediction, yDomain);
        expect(result.isVisible).toBe(true);
        expect(result.topPercent).toBeCloseTo(33.33); // (30-20)/30 * 100
        expect(result.heightPercent).toBeCloseTo(33.33); // (20-10)/30 * 100
    });

    it('should handle only upper bound', () => {
        const prediction = { index: 1, upperBound: 20 };
        const yDomain: [number, number] = [0, 30];

        const result = calculatePredictionOverlayYPosition(prediction, yDomain);
        expect(result.isVisible).toBe(true);
        expect(result.topPercent).toBeCloseTo(33.33);
        expect(result.heightPercent).toBeCloseTo(66.67);
    });

    it('should handle only lower bound', () => {
        const prediction = { index: 1, lowerBound: 10 };
        const yDomain: [number, number] = [0, 30];

        const result = calculatePredictionOverlayYPosition(prediction, yDomain);
        expect(result.isVisible).toBe(true);
        expect(result.topPercent).toBe(0);
        expect(result.heightPercent).toBeCloseTo(66.67);
    });

    it('should fallback to full height when no bounds', () => {
        const prediction = { index: 1 };
        const yDomain: [number, number] = [0, 30];

        const result = calculatePredictionOverlayYPosition(prediction, yDomain);
        expect(result.isVisible).toBe(true);
        expect(result.topPercent).toBe(0);
        expect(result.heightPercent).toBe(100);
    });
});

describe('calculateYAxisDomain', () => {
    it('should calculate domain with buffer from metrics and predictions', () => {
        const futurePredictions = [{ index: 1, lowerBound: 5, upperBound: 25 }];

        const result = calculateYAxisDomain(mockMetricData, futurePredictions, 0.1);
        const expectedMin = 5 - (25 - 5) * 0.1; // 5 - 2 = 3
        const expectedMax = 25 + (25 - 5) * 0.1; // 25 + 2 = 27

        expect(result[0]).toBeCloseTo(expectedMin);
        expect(result[1]).toBeCloseTo(expectedMax);
    });

    it('should not go below 0 for positive values when buffer would push it negative', () => {
        const positiveMetrics = [
            { x: '2024-01-01T00:00:00Z', y: 0.5, hasAnomaly: false },
            { x: '2024-01-02T00:00:00Z', y: 1, hasAnomaly: false },
        ];

        const result = calculateYAxisDomain(positiveMetrics, [], 2); // large buffer that would push min negative
        expect(result[0]).toBe(0); // should be clamped to 0
    });

    it('should return default domain when no data', () => {
        const result = calculateYAxisDomain([], [], 0.1);
        expect(result).toEqual([0, 100]);
    });
});

describe('getFillColor', () => {
    it('should return exclusion window color when in exclusion window', () => {
        const point: MetricDataPoint = {
            x: '2024-01-01T15:00:00Z',
            y: 10,
            hasAnomaly: true,
        };

        const result = getFillColor(point, mockExclusionWindows);
        expect(result).toBe(EXCLUSION_WINDOW_COLOR);
    });

    it('should return anomaly color when has anomaly and not in exclusion window', () => {
        const point: MetricDataPoint = {
            x: '2024-01-01T10:00:00Z',
            y: 10,
            hasAnomaly: true,
        };

        const result = getFillColor(point, mockExclusionWindows);
        expect(result).toBe(ANOMALY_COLOR);
    });

    it('should return metrics line color when normal point', () => {
        const point: MetricDataPoint = {
            x: '2024-01-01T10:00:00Z',
            y: 10,
            hasAnomaly: false,
        };

        const result = getFillColor(point, mockExclusionWindows);
        expect(result).toBe(METRICS_LINE_COLOR);
    });
});
