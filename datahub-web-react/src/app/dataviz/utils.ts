import dayjs from 'dayjs';
import { scaleOrdinal } from '@visx/scale';

import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from './constants';

// Mock Data Util
export const generateDateSeries = (numOfDays) =>
    Array(numOfDays)
        .fill(0)
        .map((d, i) => ({
            date: dayjs(new Date(Date.now() - 24 * 60 * 60 * 1000 * i)).format(),
            value: Math.round(Math.max(10, Math.random() * 100 || 0)),
        }));

// Status Ordinal Scale
export const statusOrdinalScale = scaleOrdinal({
    domain: ['Not Started', 'In Progress', 'Completed'],
    range: [NOT_STARTED_COLOR, IN_PROGRESS_COLOR, COMPLETED_COLOR],
});

// private utils to help with rounding y axis numbers
const NUMERICAL_ABBREVIATIONS = ['k', 'm', 'b', 't'];
function roundToPrecision(n: number, precision: number) {
    const prec = 10 ** precision;
    return Math.round(n * prec) / prec;
}

/**
 * ie. 24044 -> 24k
 * @param n
 */
export const truncateNumberForDisplay = (n: number, skipRounding?: boolean): string => {
    let base = Math.floor(Math.log(Math.abs(n)) / Math.log(1000));
    const suffix = NUMERICAL_ABBREVIATIONS[Math.min(NUMERICAL_ABBREVIATIONS.length - 1, base - 1)];
    base = NUMERICAL_ABBREVIATIONS.indexOf(suffix) + 1;
    const roundedNumber = skipRounding ? n : Math.round(n);
    return suffix ? roundToPrecision(n / 1000 ** base, 0) + suffix : `${roundedNumber}`;
};

// Number Abbreviations
export const abbreviateNumber = (str) => {
    const number = parseFloat(str);
    if (Number.isNaN(number)) return str;
    if (number < 1000) return number;
    const abbreviations = ['K', 'M', 'B', 'T'];
    const index = Math.floor(Math.log10(number) / 3);
    const suffix = abbreviations[index - 1];
    const shortNumber = number / 10 ** (index * 3);
    return `${shortNumber}${suffix}`;
};

// Byte Abbreviations
export const abbreviateBytes = (str): string => {
    const bytes = parseFloat(str);
    if (Number.isNaN(bytes)) return str;
    if (bytes < 1024) return `${bytes} B`;
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const index = Math.floor(Math.log(bytes) / Math.log(1024));
    const shortBytes = bytes / 1024 ** index;

    return `${abbreviateNumber(shortBytes.toFixed())} ${units[index]}`;
};

type CalculateYScaleExtentForChartOptions = {
    defaultYValue: number;
    // Between 0-1, represents what % of the chart's height should be empty above and below.
    // ie. if this is .1, then 10% of the chart's height will be empty.
    yScaleBufferFactor?: number;
};

/**
 * Creates a yscale range for charts, with optional buffers
 * @param yValues
 * @param options
 */
export const calculateYScaleExtentForChart = (
    yValues: number[],
    options: CalculateYScaleExtentForChartOptions = { defaultYValue: 0 },
): { min: number; max: number } => {
    let min = yValues.length ? Math.min(...yValues) : options.defaultYValue;
    let max = yValues.length ? Math.max(...yValues) : options.defaultYValue;

    // Add some extra range above and below so things aren't pushed to the edge
    const { yScaleBufferFactor } = options;
    if (yScaleBufferFactor) {
        let yScaleBuffer = 0;
        // Edge case: if max and min are the same, add some buffer above and below so the points are nicely centered
        if (max === min) {
            // Ie. Let's say max/min=1.5B. In this case we want to add ~100M buffer and and below
            // This will make the y-axis display 1.4B at the bottom and 1.6B at the top,
            // While nicely centering the data points.
            const decimalPlaceValue = max.toString().length;
            yScaleBuffer = 10 ** (decimalPlaceValue - 1);
        } else {
            // By default, the chart will put the min at the bottom edge and the max at the top edge.
            // So if yScaleBufferFactor=0.1, then we want 10% of the chart at the top and bottom to be empty
            const distance = max - min;
            const newDistance = distance / (1 - yScaleBufferFactor);
            yScaleBuffer = newDistance * yScaleBufferFactor;
        }

        min -= yScaleBuffer;
        max += yScaleBuffer;
    }

    return { min, max };
};

/**
 * Gets the px overlap between two markers
 * @param marker1
 * @param marker2
 * @returns {number | undefined} undefined if no overlap
 */
export function calculateOverlapBetweenTwoMarkers(
    marker1: { xOffset: number; width: number },
    marker2: { xOffset: number; width: number },
): undefined | number {
    let markerOverlapPx: number | undefined;

    // Take width of the left and right half of the two markers (where they will collide)
    const netWidth = marker1.width / 2 + marker2.width / 2;

    // Calculate distance and potential overlap
    const distance = Math.abs(marker1.xOffset - marker2.xOffset);
    if (distance < netWidth) {
        markerOverlapPx = netWidth - distance;
    }
    return markerOverlapPx;
}
