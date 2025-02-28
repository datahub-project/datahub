import { Col, Divider, Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { DateInterval, DateRange } from '../../../../../../../types.generated';
import { ChartCard } from '../../../../../../analyticsDashboard/components/ChartCard';
import { ChartContainer } from '../../../../../../analyticsDashboard/components/ChartContainer';
import { TimeSeriesChart } from '../../../../../../analyticsDashboard/components/TimeSeriesChart';

const ChartTitle = styled(Typography.Title)`
    && {
        margin-bottom: 20px;
        text-align: left;
        width: 100%;
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
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
        <>
            <Col sm={24} md={24} lg={8} xl={8}>
                <ChartCard $shouldScroll={false}>
                    <ChartContainer>
                        <ChartTitle level={5}>{chartData.title}</ChartTitle>
                        <ThinDivider />
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
            </Col>
        </>
    );
}
