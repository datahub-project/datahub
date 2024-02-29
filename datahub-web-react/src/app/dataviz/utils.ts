import dayjs from "dayjs";
import { scaleOrdinal } from "@visx/scale";

import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from './constants';

// Mock Data Util
export const generateCount = (totalAssets) => {
	let total = totalAssets;

	const completed = Math.floor(Math.random() * (total - 0 + 1) + 0);
	total -= completed;

	const inProgress = Math.floor(Math.random() * (total - 0 + 1) + 0);
	total -= inProgress;

	const notStarted = total;

	return ({
		'Not Started': notStarted,
		'In Progress': inProgress,
		Completed: completed,
	});
}

// Mock Data Util
export const generateDateSeries = (numOfDays) =>
	Array(numOfDays).fill(0).map((d, i) => ({
		date: dayjs(new Date(Date.now() - ((24 * 60 * 60 * 1000) * i))).format(),
		value: Math.round(Math.max(10, Math.random() * 100 || 0)),
	}));

// Status Ordinal Scale 
export const statusOrdinalScale = scaleOrdinal({
	domain: ['Completed', 'In Progress', 'Not Started'],
	range: [COMPLETED_COLOR, IN_PROGRESS_COLOR, NOT_STARTED_COLOR]
});


const ABBREVS = ['k', 'm', 'b', 't']
function roundToPrecision(n: number, precision: number) {
	var prec = 10 ** precision;
	return Math.round(n * prec) / prec;
}

/**
 * ie. 24044 -> 24k
 * @param n
 */
export const truncateNumberForDisplay = (n: number): string => {
	let base = Math.floor(Math.log(Math.abs(n)) / Math.log(1000));
	const suffix = ABBREVS[Math.min(ABBREVS.length - 1, base - 1)];
	base = ABBREVS.indexOf(suffix) + 1;
	return suffix ? roundToPrecision(n / 1000 ** base, 0) + suffix : `${Math.round(n)}`;

}
