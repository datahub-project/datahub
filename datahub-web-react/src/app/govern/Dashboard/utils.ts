import { scaleOrdinal } from '@visx/scale';

import dayjs from 'dayjs';
import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from '../../dataviz/constants';

export const DocumentationTabs = {
    ANALYTICS: 'analytics',
    FORMS: 'forms',
};

// Status Ordinal Scale
export const statusOrdinalScale = scaleOrdinal({
    domain: ['Not Started', 'In Progress', 'Completed'],
    range: [NOT_STARTED_COLOR, IN_PROGRESS_COLOR, COMPLETED_COLOR],
});

// Merge Row and Header Data
export const mergeRowAndHeaderData = (header, table) => {
    return table.map((d) => {
        const row = {};
        d.row.forEach((res, i) => {
            row[`${header[i]}`] = res.value;
        });
        return row;
    });
};

// Get Entity Info
export const getEntityInfo = (data, urn) => {
    const rows = data?.formAnalytics?.table || data?.table || data;
    if (typeof rows !== 'object') return null;

    const row = rows?.find((r) => r.row.find((res) => res.value === urn));
    if (!row) return null;

    const rowResult = row.row.find((res) => res.value === urn);
    return rowResult ? rowResult.entity : null;
};

// Percentage Util for Top/Least Perfoming Records
export const getPercentage = (data, part) => {
    const total = Number(data.completed_count) + Number(data.in_progress_count) + Number(data.not_started_count);
    return Math.round((part / total) * 100);
};

// Get percentage format
export const formatPercentage = (percentage) => `${(percentage * 100).toFixed(0).replace(/[.,]00$/, '')}%`;

// Get date format for date with date trunc aggregation
export const dateFormat = (series) => {
    let format = 'MMM D'; // last 7 days
    if (series === 30) format = 'MMM D'; // last 30 days
    if (series === 90) format = 'MMM D, YYYY'; // last 90 days
    if (series === 365) format = 'MMM YYYY'; // last 365 days
    if (series === 10000) format = 'MMM YYYY'; // all time
    return format;
};

// Truncate string
export const truncateString = (str) => {
    if (!str) return '';
    const length = 20;
    if (str.length > length) return `${str.substring(0, length - 1)}…`;
    return str;
};

export const freshnessColor = (snapshot) => {
    const now = dayjs();
    const snapshotDate = dayjs(snapshot);

    const oneMonthAgo = now.subtract(1, 'month');
    const threeDaysAgo = now.subtract(3, 'day');

    if (snapshotDate.isBefore(oneMonthAgo)) {
        return 'red';
    }
    if (snapshotDate.isBefore(threeDaysAgo)) {
        return 'orange';
    }
    return 'green';
};

// Define the sorting function
export const columnSorterFunction = (a, b, key) => {
    if (key.includes('%')) {
        return parseFloat(a[key]) - parseFloat(b[key]);
    }
    // Check if the values are strings
    if (typeof a[key] === 'string' && typeof b[key] === 'string') {
        // Case-insensitive string comparison
        return a[key].localeCompare(b[key]);
    }
    // Numeric comparison
    return a[key] - b[key];
};

// Handle url params
export const setUrlParams = (param, router, clearFilter = false) => {
    const { history, location } = router;
    const searchParams = new URLSearchParams(location.search);

    searchParams.set(param.key, param.value);
    if (clearFilter) searchParams.delete('filter');

    const newSearch = searchParams.toString();
    history.push(`${location.pathname}${newSearch ? `?${newSearch}` : ''}`);
};
