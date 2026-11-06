import { Card, Text } from '@components';
import React, { useMemo } from 'react';
import styled, { useTheme } from 'styled-components';

import { ChartContainer } from '@app/analyticsDashboardV2/components/ChartContainer';
import { TimeSeriesChart } from '@app/analyticsDashboardV2/components/TimeSeriesChart';

import { DateInterval, DateRange } from '@types';

const ChartTitle = styled(Text).attrs({ type: 'span' })`
    && {
        text-align: left;
        font-size: 12px;
        color: ${(props) => props.theme.colors.textSecondary};
        line-height: inherit;
    }
    word-break: break-word;
`;

type Point = {
    timeMs: number;
    value: number;
};

type AxisConfig = {
    formatter: (tick: number) => string;
};

type Props = {
    title: string;
    values: Array<Point>;
    tickInterval: DateInterval;
    dateRange: DateRange;
    yAxis?: AxisConfig;
    visible?: boolean;
    width?: number;
    height?: number;
    lineColor?: string;
    padding?: string;
};

const DEFAULT_AXIS_WIDTH = 2;

/**
 * Time Series Chart with a single line.
 */
export default function StatChart({
    title,
    values,
    tickInterval: interval,
    dateRange,
    yAxis,
    visible = true,
    width = 360,
    height = 300,
    lineColor,
    padding = '24px',
}: Props) {
    const theme = useTheme();
    const resolvedLineColor = lineColor || theme.colors.chartsSeafoamHigh;
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
        <Card padding={padding} style={{ visibility: visible ? 'visible' : 'hidden' }}>
            <ChartContainer>
                <ChartTitle>{title}</ChartTitle>
                <TimeSeriesChart
                    style={{
                        lineColor: resolvedLineColor,
                        axisColor: theme.colors.border,
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
                    width={width}
                    height={height}
                    yScale={{ type: 'linear', zero: false }}
                    yAxis={yAxis}
                />
            </ChartContainer>
        </Card>
    );
}
