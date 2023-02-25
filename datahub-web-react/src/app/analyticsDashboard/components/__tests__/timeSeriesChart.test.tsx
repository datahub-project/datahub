import { computeLines } from '../TimeSeriesChart';

describe('timeUtils', () => {
    describe('addInterval', () => {
        it('compute lines works works correctly for weekly case', () => {
            const chartData = {
                title: 'Weekly Active Users',
                lines: [
                    {
                        name: 'total',
                        data: [
                            {
                                x: '2023-02-20T00:00:00.000Z',
                                y: 1,
                            },
                        ],
                    },
                ],
                dateRange: {
                    start: '1672012800000',
                    end: '1677369600000',
                },
                interval: 'WEEK',
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
    });
});
