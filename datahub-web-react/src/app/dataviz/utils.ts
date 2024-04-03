import dayjs from "dayjs";
import { scaleOrdinal } from "@visx/scale";

import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from './constants';

// Mock Data Util
export const generateDateSeries = (numOfDays) =>
	Array(numOfDays).fill(0).map((d, i) => ({
		date: dayjs(new Date(Date.now() - ((24 * 60 * 60 * 1000) * i))).format(),
		value: Math.round(Math.max(10, Math.random() * 100 || 0)),
	}));

// Status Ordinal Scale 
export const statusOrdinalScale = scaleOrdinal({
	domain: ['Not Started', 'In Progress', 'Completed'],
	range: [NOT_STARTED_COLOR, IN_PROGRESS_COLOR, COMPLETED_COLOR]
});



// private utils to help with rounding y axis numbers
const NUMERICAL_ABBREVIATIONS = ['k', 'm', 'b', 't']
function roundToPrecision(n: number, precision: number) {
	const prec = 10 ** precision;
	return Math.round(n * prec) / prec;
}

/**
 * ie. 24044 -> 24k
 * @param n
 */
export const truncateNumberForDisplay = (n: number): string => {
	let base = Math.floor(Math.log(Math.abs(n)) / Math.log(1000));
	const suffix = NUMERICAL_ABBREVIATIONS[Math.min(NUMERICAL_ABBREVIATIONS.length - 1, base - 1)];
	base = NUMERICAL_ABBREVIATIONS.indexOf(suffix) + 1;
	return suffix ? roundToPrecision(n / 1000 ** base, 0) + suffix : `${Math.round(n)}`;

}

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
}

/**
 * Creates a yscale range for charts, with optional buffers
 * @param yValues 
 * @param options 
 */
export const calculateYScaleExtentForChart = (yValues: number[], options: { defaultYValue: number, includeBufferWithOptions?: (extent: { min: number, max: number }) => { axisTicksCount: number } | undefined } = { defaultYValue: 0 }): { min: number, max: number } => {

	let min = yValues.length ? Math.min(...yValues) : options.defaultYValue;
	let max = yValues.length ? Math.max(...yValues) : options.defaultYValue;

	// Add some extra range above and below if the min and the max are the same so things are nicely centered
	const maybeBufferOptions = options.includeBufferWithOptions?.({ min, max })
	if (maybeBufferOptions) {
		const averageValue = (min + max) / 2
		const averageValueBase = Math.floor(averageValue).toString().length
		let differentiator = 10 ** (averageValueBase - 1)
		differentiator /= maybeBufferOptions.axisTicksCount
		min -= differentiator;
		max += differentiator;
	}

	return { min, max }
}


/**
 * Gets the px overlap between two markers
 * @param marker1 
 * @param marker2 
 * @returns {number | undefined} undefined if no overlap
 */
export function calculateOverlapBetweenTwoMarkers(marker1: { xOffset: number, width: number }, marker2: { xOffset: number, width: number }): undefined | number {
	let markerOverlapPx: number | undefined;

	// Take width of the left and right half of the two markers (where they will collide)
	const netWidth = (marker1.width / 2) + (marker2.width / 2);

	// Calculate distance and potential overlap
	const distance = Math.abs(marker1.xOffset - marker2.xOffset);
	if (distance < netWidth) {
		markerOverlapPx = netWidth - distance;
	}
	return markerOverlapPx;
}
