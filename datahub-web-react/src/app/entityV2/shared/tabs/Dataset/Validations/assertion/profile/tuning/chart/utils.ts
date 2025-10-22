import { scaleTime } from '@visx/scale';
import moment from 'moment';

import { AssertionPrediction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/useInferenceRegenerationPoller';
import {
    ANOMALY_COLOR,
    EXCLUSION_WINDOW_COLOR,
    METRICS_LINE_COLOR,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/constants';

import { AssertionExclusionWindow, AssertionExclusionWindowType } from '@types';

export type MetricDataPoint = {
    x: string; // ISO date string
    y: number; // metric value
    hasAnomaly: boolean; // whether this point has an anomaly
};

export type VisibleExclusionWindow = {
    leftPercent: number;
    widthPercent: number;
    startTime: number;
    endTime: number;
};

/**
 * Filter predictions to only include future ones, optionally within a date range
 */
export const getFuturePredictions = (
    predictions: AssertionPrediction[],
    dateRange?: [moment.Moment | null, moment.Moment | null],
): AssertionPrediction[] => {
    const now = Date.now();
    return predictions
        .filter((prediction) => {
            if (!prediction.timeWindow?.startTimeMillis) return false;

            // Must be in the future
            if (prediction.timeWindow.startTimeMillis <= now) return false;

            // If date range is specified, filter to only include predictions within that range
            if (dateRange && dateRange[0] && dateRange[1]) {
                const rangeStart = dateRange[0].valueOf();
                const rangeEnd = dateRange[1].valueOf();
                const predictionStart = prediction.timeWindow.startTimeMillis;
                const predictionEnd = prediction.timeWindow.endTimeMillis || predictionStart;

                // Include if prediction overlaps with the date range
                return predictionStart <= rangeEnd && predictionEnd >= rangeStart;
            }

            return true;
        })
        .sort((a, b) => {
            const aStart = a.timeWindow?.startTimeMillis || 0;
            const bStart = b.timeWindow?.startTimeMillis || 0;
            return aStart - bStart;
        });
};

/**
 * Create x-axis domain based on selected date range or data range
 */
export const calculateXAxisDomain = (
    dateRange: [moment.Moment | null, moment.Moment | null],
    metrics: MetricDataPoint[],
    futurePredictions: AssertionPrediction[],
): [Date, Date] => {
    let startTime: Date;
    let endTime: Date;

    if (dateRange[0] && dateRange[1]) {
        // Use selected date range as base
        startTime = dateRange[0].toDate();
        endTime = dateRange[1].toDate();
    } else if (metrics.length > 0) {
        // Fall back to data range
        const dataMinTime = Math.min(...metrics.map((d) => new Date(d.x).getTime()));
        const dataMaxTime = Math.max(...metrics.map((d) => new Date(d.x).getTime()));
        startTime = new Date(dataMinTime);
        endTime = new Date(dataMaxTime);
    } else {
        // Default fallback
        return [new Date(), new Date()];
    }

    // Always extend domain to include future predictions if they exist
    if (futurePredictions.length > 0) {
        const latestPredictionTime = Math.max(
            ...futurePredictions.map((p) => p.timeWindow?.endTimeMillis || p.timeWindow?.startTimeMillis || 0),
        );
        endTime = new Date(Math.max(endTime.getTime(), latestPredictionTime));
    }

    return [startTime, endTime];
};

/**
 * Create time scale for zoom functionality
 */
export const createTimeScale = (xAxisDomain: [Date, Date], width: number, MARGIN: { left: number; right: number }) => {
    return scaleTime({
        domain: [xAxisDomain[0].getTime(), xAxisDomain[1].getTime()],
        range: [MARGIN.left, width - MARGIN.right],
    });
};

/**
 * Utility function to calculate tooltip position for prediction overlays
 */
export const calculateTooltipPosition = (
    predictionRect: DOMRect,
    chartRect: DOMRect,
    height: number,
    MARGIN: { top: number; bottom: number },
) => {
    const tooltipWidth = 200;

    // Calculate position relative to chart container
    const relativeX = predictionRect.left - chartRect.left + predictionRect.width / 2;

    // Position tooltip in the center of the shaded area
    const chartAreaHeight = height - MARGIN.top - MARGIN.bottom;
    const centerY = MARGIN.top + chartAreaHeight / 2;
    const relativeY = centerY;

    // Calculate ideal centered position for tooltip
    let tooltipX = relativeX - tooltipWidth / 2;

    // Adjust if tooltip would go off the right edge of chart
    const chartWidth = chartRect.width;
    if (tooltipX + tooltipWidth > chartWidth - 20) {
        tooltipX = chartWidth - tooltipWidth - 20;
    }

    // Adjust if tooltip would go off the left edge of chart
    if (tooltipX < 20) {
        tooltipX = 20;
    }

    return {
        x: tooltipX,
        y: relativeY,
    };
};

/**
 * Format date for tooltip display
 */
export const formatTooltipDate = (timestamp: number): string => {
    return new Date(timestamp).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
    });
};

/**
 * Format date for axis display
 */
export const formatAxisDate = (value: number): string => {
    const date = new Date(value);
    return date.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
    });
};

/**
 * Format numbers rounded to 4 decimal places with automatic comma formatting
 */
export const formatMetricNumber = (value: number): string => {
    // Round to 4 decimal places using toFixed, then convert back to number
    const rounded = Number.parseFloat(value.toFixed(4));
    return rounded.toLocaleString();
};

/**
 * Check if a timestamp falls within any exclusion window
 */
export const isTimestampInExclusionWindow = (
    timestamp: number,
    exclusionWindows: AssertionExclusionWindow[],
): boolean => {
    return exclusionWindows.some((window) => {
        if (window.type === AssertionExclusionWindowType.FixedRange && window.fixedRange) {
            const { startTimeMillis, endTimeMillis } = window.fixedRange;
            return timestamp >= startTimeMillis && timestamp <= endTimeMillis;
        }
        // TODO: Implement weekly and holiday exclusion window checks
        // For now, we'll only handle fixed range exclusions
        return false;
    });
};

/**
 * Find which exclusion window a timestamp falls within
 */
export const getExclusionWindowForTimestamp = (
    timestamp: number,
    exclusionWindows: AssertionExclusionWindow[],
): AssertionExclusionWindow | null => {
    return (
        exclusionWindows.find((window) => {
            if (window.type === AssertionExclusionWindowType.FixedRange && window.fixedRange) {
                const { startTimeMillis, endTimeMillis } = window.fixedRange;
                return timestamp >= startTimeMillis && timestamp <= endTimeMillis;
            }
            // TODO: Implement weekly and holiday exclusion window checks
            // For now, we'll only handle fixed range exclusions
            return false;
        }) || null
    );
};

/**
 * Calculate visible exclusion windows within the current x-axis domain
 */
export const calculateVisibleExclusionWindows = (
    exclusionWindows: AssertionExclusionWindow[],
    xAxisDomain: [Date, Date],
): VisibleExclusionWindow[] => {
    const domainStart = xAxisDomain[0].getTime();
    const domainEnd = xAxisDomain[1].getTime();
    const totalDuration = domainEnd - domainStart;

    return exclusionWindows
        .filter((window) => {
            // For now, only handle fixed range exclusion windows
            if (window.type === AssertionExclusionWindowType.FixedRange && window.fixedRange) {
                const { startTimeMillis, endTimeMillis } = window.fixedRange;
                // Check if the window overlaps with the current domain
                return startTimeMillis < domainEnd && endTimeMillis > domainStart;
            }
            return false;
        })
        .map((window) => {
            if (window.type === AssertionExclusionWindowType.FixedRange && window.fixedRange) {
                const { startTimeMillis, endTimeMillis } = window.fixedRange;

                // Calculate position relative to domain
                const startOffset = Math.max(0, (startTimeMillis - domainStart) / totalDuration);
                const endOffset = Math.min(1, (endTimeMillis - domainStart) / totalDuration);
                const duration = endOffset - startOffset;

                return {
                    leftPercent: startOffset * 100,
                    widthPercent: duration * 100,
                    startTime: startTimeMillis,
                    endTime: endTimeMillis,
                };
            }
            return null;
        })
        .filter((item): item is NonNullable<typeof item> => item !== null);
};

/**
 * Calculate the position and width for a prediction overlay
 */
export const calculatePredictionOverlayPosition = (
    startTimeMillis: number,
    endTimeMillis: number,
    xAxisDomain: [Date, Date],
): { leftPercent: number; widthPercent: number; isVisible: boolean } => {
    const domainStart = xAxisDomain[0].getTime();
    const domainEnd = xAxisDomain[1].getTime();
    const totalDuration = domainEnd - domainStart;

    const startOffset = (startTimeMillis - domainStart) / totalDuration;
    const duration = (endTimeMillis - startTimeMillis) / totalDuration;

    // Check if the prediction is visible in the current domain
    const isVisible = !(startOffset > 1 || startOffset + duration < 0);

    if (!isVisible) {
        return { leftPercent: 0, widthPercent: 0, isVisible: false };
    }

    const leftPercent = Math.max(0, startOffset * 100);
    const widthPercent = Math.min(100 - leftPercent, duration * 100);

    return { leftPercent, widthPercent, isVisible: true };
};

/**
 * Calculate the Y position and height for a prediction overlay based on prediction bounds
 */
export const calculatePredictionOverlayYPosition = (
    prediction: AssertionPrediction,
    yAxisDomain: [number, number],
): { topPercent: number; heightPercent: number; isVisible: boolean } => {
    const { lowerBound, upperBound } = prediction;

    // If no bounds are available, fallback to full height
    if (lowerBound === undefined && upperBound === undefined) {
        return { topPercent: 0, heightPercent: 100, isVisible: true };
    }

    const [yMin, yMax] = yAxisDomain;
    const yRange = yMax - yMin;

    let topY: number;
    let bottomY: number;

    if (upperBound !== undefined && lowerBound !== undefined) {
        // Use both bounds
        topY = upperBound;
        bottomY = lowerBound;
    } else if (upperBound !== undefined) {
        // Only upper bound - extend to bottom of domain
        topY = upperBound;
        bottomY = yMin;
    } else if (lowerBound !== undefined) {
        // Only lower bound - extend to top of domain
        topY = yMax;
        bottomY = lowerBound;
    } else {
        // Fallback to full height
        return { topPercent: 0, heightPercent: 100, isVisible: true };
    }

    // Ensure bounds are within the domain
    topY = Math.min(yMax, Math.max(yMin, topY));
    bottomY = Math.min(yMax, Math.max(yMin, bottomY));

    // Calculate positions as percentages from top
    const topFromDomainTop = (yMax - topY) / yRange;
    const bottomFromDomainTop = (yMax - bottomY) / yRange;

    const topPercent = topFromDomainTop * 100;
    const heightPercent = (bottomFromDomainTop - topFromDomainTop) * 100;

    // Check if the prediction overlay is visible
    const isVisible = heightPercent > 0 && topPercent < 100 && topPercent + heightPercent > 0;

    return {
        topPercent: Math.max(0, topPercent),
        heightPercent: Math.max(0, Math.min(100 - Math.max(0, topPercent), heightPercent)),
        isVisible,
    };
};

/**
 * Calculate y-axis domain with buffer based on metrics and predictions
 */
export const calculateYAxisDomain = (
    metrics: MetricDataPoint[],
    futurePredictions: AssertionPrediction[],
    bufferPercent = 0.1,
): [number, number] => {
    const allValues: number[] = [];

    // Add metric values
    allValues.push(...metrics.map((point) => point.y));

    // Add prediction bounds
    futurePredictions.forEach((prediction) => {
        if (prediction.lowerBound !== undefined) {
            allValues.push(prediction.lowerBound);
        }
        if (prediction.upperBound !== undefined) {
            allValues.push(prediction.upperBound);
        }
    });

    // If no values, return a default domain
    if (allValues.length === 0) {
        return [0, 100];
    }

    const minValue = Math.min(...allValues);
    const maxValue = Math.max(...allValues);

    // Calculate buffer amount based on the data range
    const range = maxValue - minValue;
    const buffer = range * bufferPercent;

    // Apply buffer, but ensure we don't go below 0 if all values are positive
    const minWithBuffer = minValue >= 0 && minValue - buffer < 0 ? 0 : minValue - buffer;
    const maxWithBuffer = maxValue + buffer;

    return [minWithBuffer, maxWithBuffer];
};

/**
 * Helper function to determine fill color based on anomaly and exclusion window status
 */
export const getFillColor = (point: MetricDataPoint, exclusionWindows: AssertionExclusionWindow[]): string => {
    const isInExclusionWindow = isTimestampInExclusionWindow(new Date(point.x).getTime(), exclusionWindows);

    if (isInExclusionWindow) {
        return EXCLUSION_WINDOW_COLOR;
    }

    if (point.hasAnomaly) {
        return ANOMALY_COLOR;
    }

    return METRICS_LINE_COLOR;
};
