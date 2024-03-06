import { scaleOrdinal } from "@visx/scale";

import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from '../../dataviz/constants';

// Status Ordinal Scale 
export const statusOrdinalScale = scaleOrdinal({
	domain: ['Not Started', 'In Progress', 'Completed'],
	range: [NOT_STARTED_COLOR, IN_PROGRESS_COLOR, COMPLETED_COLOR]
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

// Get date format for date with date trunc aggregation
export const dateFormat = (series) => {
	let format = 'MMM D'; // last 7 days
	if (series === 30) format = 'MMM D'; // last 30 days
	if (series === 90) format = 'MMMM'; // last 90 days
	if (series === 365) format = 'MM/YYYY'; // last 365 days
	return format;
}

// Truncate string
export const truncateString = (str) => {
	if (!str) return '';
	const length = 20;
	if (str.length > length) return `${str.substring(0, length - 1)}…`;
	return str;
}