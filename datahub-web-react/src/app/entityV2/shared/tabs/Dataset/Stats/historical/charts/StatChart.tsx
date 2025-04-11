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
        font-size: 12px;
        color: ${ANTD_GRAY[8]};
    }
`;

/**
 * Exported this styled card to enable width adjustment for better fit within the sidebar also for boxShadow.
 * This approach allows style updates without the need for unnecessary props.
 */
export const ChartCard = styled(Card)<{ visible: boolean }>`
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
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
    width?: number;
    height?: number;
    lineColor?: string;
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
export default function StatChart({
    title,
    values,
    tickInterval: interval,
    dateRange,
    yAxis,
    visible = true,
    width = 360,
    height = 300,
    lineColor = DEFAULT_LINE_COLOR,
}: Props) {
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
                        lineColor,
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
                    width={width}
                    height={height}
                    yScale={{ type: 'linear', zero: false }}
                    yAxis={yAxis}
                />
            </ChartContainer>
        </ChartCard>
    );
}
