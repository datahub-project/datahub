import {
    AnalyticsChart,
    AnalyticsChartGroup,
    BarSegment,
    DateInterval,
    NamedBar,
    NamedLine,
    NumericDataPoint,
    TableChart,
} from '../../types.generated';

const dateOffset = 24 * 60 * 60 * 1000; // 1 day

function generatePoints(length: number, start: Date, interval: number) {
    const output: NumericDataPoint[] = [];
    const iterateDate = new Date(start);
    let iterateVal = Math.random() * 20;
    for (let i = 0; i < length; i++) {
        iterateVal += Math.random() * 5;
        output.push({
            y: iterateVal,
            x: iterateDate.toString(),
        });
        iterateDate.setTime(iterateDate.getTime() + interval);
    }
    return output;
}

export function generateSampleTimeSeries(title: string, lines: number): AnalyticsChart {
    const allLines: NamedLine[] = [];
    for (let i = 0; i < lines; i++) {
        const lineTitle = Math.random().toString(36).substring(9);
        allLines.push({
            name: `${lineTitle} per day`,
            data: generatePoints(8, new Date(), dateOffset),
        });
    }

    return {
        title,
        lines: allLines,
        dateRange: {
            start: new Date().getTime().toString(),
            end: new Date(new Date().getDate() - 6).getTime().toString(),
        },
        interval: DateInterval.Day,
        __typename: 'TimeSeriesChart',
    };
}

function generateBars(length: number) {
    const output: BarSegment[] = [];
    let iterateVal = Math.random() * 20;
    for (let i = 0; i < length; i++) {
        iterateVal += Math.random() * 5;
        output.push({
            value: iterateVal,
            label: `Segment ${i}`,
        });
    }
    return output;
}

export function generateSampleBarChart(title: string, lines: number): AnalyticsChart {
    const allBars: NamedBar[] = [];
    for (let i = 0; i < lines; i++) {
        const barTitle = Math.random().toString(36).substring(9);
        allBars.push({
            name: `${barTitle} per day`,
            segments: generateBars(11),
        });
    }

    return {
        title,
        bars: allBars,
        __typename: 'BarChart',
    };
}

export function generateSampleTableChart(title: string): TableChart {
    return {
        title,
        columns: ['Query', '# Searches', '% Searches'],
        rows: [
            { values: ['lineage', '331', '4.34%'] },
            { values: ['presto', '22', '4.34%'] },
            { values: ['snowflake', '12', '4.34%'] },
        ],
        __typename: 'TableChart',
    };
}

export function generateSampleChartGroup(title: string, charts: number): AnalyticsChartGroup {
    const constructedCharts: AnalyticsChart[] = [];
    for (let i = 0; i < charts; i++) {
        const chartTitle = Math.random().toString(36).substring(9);
        constructedCharts.push(generateSampleTimeSeries(`${chartTitle} over time`, Math.floor(Math.random() * 3 + 4)));
    }
    constructedCharts.push(generateSampleBarChart('Bar Sample', 7));
    constructedCharts.push(generateSampleTableChart('Top Search Results'));

    return {
        title,
        charts: constructedCharts,
    };
}

export const sampleHighlights = [
    {
        value: 42,
        title: 'Weekly Active Users',
        body: '22% increase vs last week',
    },
];

export const sampleCharts: AnalyticsChartGroup[] = [
    generateSampleChartGroup('Overview', 3),
    generateSampleChartGroup('Searches', 6),
    generateSampleChartGroup('Entity Detail', 4),
];
