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