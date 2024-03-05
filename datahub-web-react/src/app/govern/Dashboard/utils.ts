import dayjs from "dayjs";
import { scaleOrdinal } from "@visx/scale";

import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from '../../dataviz/constants';

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

// Merge Row and Header Data
export const mergeRowAndHeaderData = (header, table) => {
	return table.map((d) => {
		const row = {};
		d.row.forEach((r, i) => { row[`${header[i]}`] = r });
		return row;
	});
}

// Get Entity Info
export const getEntityInfo = (data, urn) => {
	const rows = data?.formAnalytics?.table || data?.table || data || [];
	const row = rows.find((r) => r.row.includes(urn));

	if (!row) return null;

	const richRow = row.richRow.find((rich) => rich.value === urn);
	return richRow ? richRow.entity : null;
}

// Percentage Util for Top/Least Perfoming Records
export const getPercentage = (data, part) => {
	const total = Number(data.completed_count) + Number(data.in_progress_count) + Number(data.not_started_count);
	return Math.round((part / total) * 100);
};

// Get percentage format
export const formatPercentage = (percentage) => `${(percentage * 100).toFixed(0).replace(/[.,]00$/, "")}%`;