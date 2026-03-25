import { Card, Typography } from 'antd';
import React, { useMemo } from 'react';
import styled, { useTheme } from 'styled-components';

import { ChartContainer } from '@app/analyticsDashboard/components/ChartContainer';
import { TimeSeriesChart } from '@app/analyticsDashboard/components/TimeSeriesChart';

import { DateInterval, DateRange } from '@types';

const ChartTitle = styled(Typography.Text)`
    && {
        text-align: left;
        font-size: 14px;
        color: ${(props) => props.theme.colors.textSecondary};
    }
`;

const ChartCard = styled(Card)<{ visible: boolean }>`
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    visibility: ${(props) => (props.visible ? 'visible' : 'hidden')};
`;

type Point = {
    timeMs: number;
    value: number;
};

type AxisConfig = {
    formatter: (tick: number) => string;
};

export type Props = {
    title: string;
    values: Array<Point>;
    tickInterval: DateInterval;
    dateRange: DateRange;
    yAxis?: AxisConfig;
    visible?: boolean;
};

const DEFAULT_AXIS_WIDTH = 2;

/**
 * Time Series Chart with a single line.
 */
export default function StatChart({ title, values, tickInterval: interval, dateRange, yAxis, visible = true }: Props) {
    const theme = useTheme();
    const DEFAULT_LINE_COLOR = theme.colors.chartsBlueMedium;
    const DEFAULT_AXIS_COLOR = theme.colors.border;
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

    return (
        <ChartCard visible={visible}>
            <ChartContainer>
                <ChartTitle>{title}</ChartTitle>
                <TimeSeriesChart
                    style={{
                        lineColor: DEFAULT_LINE_COLOR,
                        axisColor: DEFAULT_AXIS_COLOR,
                        axisWidth: DEFAULT_AXIS_WIDTH,
                    }}
                    hideLegend
                    chartData={{
                        title,
                        lines: [
                            {
                                name: 'line_1',
                                data: timeSeriesData,
                            },
                        ],
                        interval,
                        dateRange,
                    }}
                    width={360}
                    height={300}
                    yScale={{ type: 'linear', zero: false }}
                    yAxis={yAxis}
                />
            </ChartContainer>
        </ChartCard>
    );
}
