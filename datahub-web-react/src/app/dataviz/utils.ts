import { scaleOrdinal } from '@visx/scale';
import dayjs from 'dayjs';

import { COMPLETED_COLOR, IN_PROGRESS_COLOR, NOT_STARTED_COLOR } from '@app/dataviz/constants';

// private utils to help with rounding y axis numbers
const NUMERICAL_ABBREVIATIONS = ['k', 'm', 'b', 't'];
function roundToPrecision(n: number, precision: number) {
    const prec = 10 ** precision;
    return Math.round(n * prec) / prec;
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
};

type CalculateYScaleExtentForChartOptions = {
    defaultYValue: number;
    // Between 0-1, represents what % of the chart's height should be empty above and below.
    // ie. if this is .1, then 10% of the chart's height will be empty.
    yScaleBufferFactor?: number;
};
