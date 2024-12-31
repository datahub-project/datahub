import { DateInterval, TimeSeriesChart } from '../../../../types.generated';
import { computeLines } from '../TimeSeriesChart';

describe('timeSeriesChart', () => {
    describe('computeLines', () => {
        it('compute lines works works correctly for weekly case', () => {
            const chartData: TimeSeriesChart = {
                title: 'Weekly Active Users',
                lines: [
                    {
                        name: 'total',
                        data: [{ x: '2023-02-20T00:00:00.000Z', y: 1 }],
                    },
                ],
                dateRange: {
                    start: '1672012800000',
                    end: '1677369600000',
                },
                interval: DateInterval.Week,
            };
            const result = computeLines(chartData, true);
            expect(result[0]).toEqual({
                name: 'total',
                data: [
                    { x: '2022-12-26T00:00:00.000Z', y: 0 },
                    { x: '2023-01-02T00:00:00.000Z', y: 0 },
                    { x: '2023-01-09T00:00:00.000Z', y: 0 },
                    { x: '2023-01-16T00:00:00.000Z', y: 0 },
                    { x: '2023-01-23T00:00:00.000Z', y: 0 },
                    { x: '2023-01-30T00:00:00.000Z', y: 0 },
                    { x: '2023-02-06T00:00:00.000Z', y: 0 },
                    { x: '2023-02-13T00:00:00.000Z', y: 0 },
                    { x: '2023-02-20T00:00:00.000Z', y: 1 },
                ],
            });
        });

        it('compute lines works works correctly for monthly case', () => {
            const chartData: TimeSeriesChart = {
                title: 'Weekly Active Users',
                lines: [
                    {
                        name: 'total',
                        data: [
                            { x: '2023-01-01T00:00:00.000Z', y: 49 },
                            { x: '2023-02-01T00:00:00.000Z', y: 52 },
                            { x: '2023-03-01T00:00:00.000Z', y: 16 },
                        ],
                    },
                ],
                dateRange: {
                    start: '1648771200000',
                    end: '1680307199999',
                },
                interval: DateInterval.Month,
            };
            const result = computeLines(chartData, true);
            expect(result[0]).toEqual({
                name: 'total',
                data: [
                    { x: '2022-04-01T00:00:00.000Z', y: 0 },
                    { x: '2022-05-01T00:00:00.000Z', y: 0 },
                    { x: '2022-06-01T00:00:00.000Z', y: 0 },
                    { x: '2022-07-01T00:00:00.000Z', y: 0 },
                    { x: '2022-08-01T00:00:00.000Z', y: 0 },
                    { x: '2022-09-01T00:00:00.000Z', y: 0 },
                    { x: '2022-10-01T00:00:00.000Z', y: 0 },
                    { x: '2022-11-01T00:00:00.000Z', y: 0 },
                    { x: '2022-12-01T00:00:00.000Z', y: 0 },
                    { x: '2023-01-01T00:00:00.000Z', y: 49 },
                    { x: '2023-02-01T00:00:00.000Z', y: 52 },
                    { x: '2023-03-01T00:00:00.000Z', y: 16 },
                ],
            });
        });
    });
});
