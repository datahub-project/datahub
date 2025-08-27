import {
    MAX_VALUE_AGGREGATION,
    SUM_VALUES_AGGREGATION,
    TimeInterval,
    getPopoverTimeFormat,
    getXAxisTickFormat,
    groupTimeData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';

const barChartData = [
    {
        time: 1735476470920,
        value: 100,
    },
    {
        time: 1735390070920,
        value: 55,
    },
];

const getGroupedData = (
    data: {
        time: number;
        value: number;
    }[],
    interval: TimeInterval,
) => {
    return groupTimeData(
        data,
        interval,
        (d) => d.time,
        (d) => d.value,
        interval === TimeInterval.DAY ? MAX_VALUE_AGGREGATION : SUM_VALUES_AGGREGATION,
    );
};

describe('get x-axis tick format for bar chart', () => {
    it('get x-axis tick format for month interval', () => {
        const groupedData = getGroupedData(barChartData, TimeInterval.MONTH);
        expect(getXAxisTickFormat(TimeInterval.MONTH, groupedData[0].time)).toEqual('Dec 24');
    });
    it('get x-axis tick format for week interval in the same month', () => {
        const groupedData = getGroupedData(barChartData, TimeInterval.WEEK);
        expect(getXAxisTickFormat(TimeInterval.WEEK, groupedData[1].time)).toEqual('22-28 Dec');
    });
    it('get x-axis tick format for week interval across 2 months', () => {
        const groupedData = getGroupedData(barChartData, TimeInterval.WEEK);
        expect(getXAxisTickFormat(TimeInterval.WEEK, groupedData[0].time)).toEqual('29-4 Dec-Jan');
    });
    it('get x-axis tick format for day interval', () => {
        const groupedData = getGroupedData(barChartData, TimeInterval.DAY);
        expect(getXAxisTickFormat(TimeInterval.DAY, groupedData[0].time)).toEqual('29 Dec');
    });
});

describe('get popover time format for bar chart', () => {
    it('get popover time format for month interval', () => {
        const groupedData = getGroupedData(barChartData, TimeInterval.MONTH);
        expect(getPopoverTimeFormat(TimeInterval.MONTH, groupedData[0].time)).toEqual('December ’2024');
    });
    it('get popover time format for week interval', () => {
        const groupedData = getGroupedData(barChartData, TimeInterval.WEEK);
        expect(getPopoverTimeFormat(TimeInterval.WEEK, groupedData[0].time)).toEqual(
            'Week of Dec. 29 ’24 - Jan. 4 ’25',
        );
    });
    it('get popover time format for day interval', () => {
        const groupedData = getGroupedData(barChartData, TimeInterval.DAY);
        expect(getPopoverTimeFormat(TimeInterval.DAY, groupedData[0].time)).toEqual('Sunday. Dec. 29 ’24');
    });
});
