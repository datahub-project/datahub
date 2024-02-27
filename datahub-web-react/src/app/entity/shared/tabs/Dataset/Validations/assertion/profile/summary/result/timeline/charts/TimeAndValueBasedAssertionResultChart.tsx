import React, { useMemo } from 'react';

import { Popover } from 'antd';
import { AreaClosed, Bar } from '@visx/shape';
import { Group } from '@visx/group';
import { LinearGradient } from '@visx/gradient';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { Maybe } from 'graphql/jsutils/Maybe';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { Assertion, AssertionResultType } from '../../../../../../../../../../../../types.generated';
import { generateTimeScaleTickValues, generateYScaleTickValues, getCustomTimeScaleTickValue, getFillColor } from './utils';
import { AssertionDataPoint, TimeRange } from '../types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import { scaleLinear } from 'd3-scale';
import { AnimatedAxis, AnimatedGrid, AnimatedLineSeries, Tooltip, XYChart } from '@visx/xychart';
import { curveMonotoneX } from '@visx/curve';

type Props = {
    data: {
        dataPoints: AssertionDataPoint[],
        context: {
            assertion: Assertion
        }
    };
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    }
};


const CHART_AXIS_LEFT_WIDTH = 48;
const CHART_AXIS_BOTTOM_HEIGHT = 40;
const ACCENT_COLOR = '#c574db'

export const TimeAndValueBasedAssertionResultChart = ({ data, timeRange, chartDimensions }: Props) => {
    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT
    const chartInnerWidth = chartDimensions.width - CHART_AXIS_LEFT_WIDTH

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, chartInnerWidth],
            }),
        [timeRange, chartInnerWidth],
    );

    const yMax = useMemo(
        () => Math.max(...data.dataPoints.map(point => point.result.yValue ?? 0)),
        [data.dataPoints]
    )
    const yScale = useMemo(
        () =>
            scaleLinear(
                [0, yMax], // can also use the 'extent' function from d3
                [chartInnerHeight, 0],
            ),
        [yMax, chartInnerHeight]
    );

    return (
        <>
            <svg width={chartDimensions.width} height={chartDimensions.height}>
                <Group left={CHART_AXIS_LEFT_WIDTH} top={0}>
                    {data.dataPoints.map(dataPoint => {
                        const barWidth = 8;
                        const barX = xScale(new Date(dataPoint.time));
                        const topOffset = yScale(dataPoint.result.yValue ?? 0);
                        const fillColor = getFillColor(dataPoint.result.type);
                        return (
                            <LinkWrapper key={dataPoint.time} to={dataPoint.result.resultUrl} target="_blank">
                                <Popover
                                    key={dataPoint.time}
                                    title={undefined}
                                    overlayStyle={{
                                        maxWidth: 440,
                                        wordWrap: 'break-word',
                                    }}
                                    content={<AssertionResultPopoverContent
                                        assertion={data.context.assertion}
                                        run={dataPoint.relatedRunEvent}
                                    />}
                                    showArrow={false}
                                >
                                    <Bar
                                        key={`bar-${dataPoint.time}`}
                                        x={barX}
                                        y={topOffset}
                                        stroke="white"
                                        width={barWidth}
                                        height={chartInnerHeight - topOffset}
                                        fill={fillColor}
                                    />
                                </Popover>
                            </LinkWrapper>
                        );
                    })}

                    <AxisLeft
                        scale={yScale}
                        stroke={ANTD_GRAY[5]}
                        tickStroke={ANTD_GRAY[5]}
                        numTicks={2}
                        tickLabelProps={(_) => ({
                            fill: ANTD_GRAY[9],
                            fontSize: 11,
                            textAnchor: 'end',
                        })}
                    />
                    <AxisBottom
                        top={chartInnerHeight}
                        scale={xScale}
                        stroke={ANTD_GRAY[5]}
                        tickStroke={ANTD_GRAY[5]}
                        tickValues={generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs)}
                        tickFormat={(v) => getCustomTimeScaleTickValue(v, timeRange)}
                        tickLabelProps={(_) => ({
                            fill: ANTD_GRAY[9],
                            fontSize: 11,
                            textAnchor: 'middle',
                        })}
                    />
                </Group>
            </svg>
        </>
    );
};
