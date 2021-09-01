import { Card, Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { DateInterval, DateRange } from '../../../../../../../../types.generated';
import { ChartContainer } from '../../../../../../../analyticsDashboard/components/ChartContainer';
import { TimeSeriesChart } from '../../../../../../../analyticsDashboard/components/TimeSeriesChart';
import { ANTD_GRAY } from '../../../../../constants';

const ChartTitle = styled(Typography.Text)`
    && {
        text-align: left;
        font-size: 14px;
        color: ${ANTD_GRAY[8]};
    }
`;

const ChartCard = styled(Card)`
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

type Point = {
    timeMs: number;
    value: number;
};

export type Props = {
    title: string;
    values: Array<Point>;
    tickInterval: DateInterval;
    dateRange: DateRange;
};

/**
 * Change these to change the chart axis & line colors
 * TODO: Add this to the theme config.
 */
const DEFAULT_LINE_COLOR = '#20d3bd';
const DEFAULT_AXIS_COLOR = '#D8D8D8';
const DEFAULT_AXIS_WIDTH = 2;

/**
 * Time Series Chart with a single line.
 */
export default function StatChart({ title, values, tickInterval: interval, dateRange }: Props) {
    const timeSeriesData = useMemo(
        () =>
            values
                .sort((a, b) => a.timeMs - b.timeMs)
                .map((value) => {
                    const dateStr = new Date(value.timeMs).toISOString();
                    return {
                        x: dateStr,
                        y: value.value,
                    };
                }),
        [values],
    );

    const chartData = {
        title,
        lines: [
            {
                name: 'line_1',
                data: timeSeriesData,
            },
        ],
        interval,
        dateRange,
    };

    return (
        <ChartCard>
            <ChartContainer>
                <ChartTitle>{chartData.title}</ChartTitle>
                <TimeSeriesChart
                    style={{
                        lineColor: DEFAULT_LINE_COLOR,
                        axisColor: DEFAULT_AXIS_COLOR,
                        axisWidth: DEFAULT_AXIS_WIDTH,
                    }}
                    hideLegend
                    chartData={chartData}
                    width={360}
                    height={300}
                />
            </ChartContainer>
        </ChartCard>
    );
}
