import React, { useMemo } from 'react';

import { Popover } from 'antd';
import { AreaClosed, Bar, Circle, LinePath } from '@visx/shape';
import { Group } from '@visx/group';
import { LinearGradient } from '@visx/gradient';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { Maybe } from 'graphql/jsutils/Maybe';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { Assertion, AssertionResultType } from '../../../../../../../../../../../../types.generated';
import { ACCENT_COLOR_HEX, generateTimeScaleTickValues, generateYScaleTickValues, getCustomTimeScaleTickValue, getFillColor } from './utils';
import { AssertionDataPoint, TimeRange } from '../types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import { scaleLinear } from 'd3-scale';
import { AnimatedAxis, AnimatedGrid, AnimatedLineSeries, Tooltip, XYChart } from '@visx/xychart';
import { curveCatmullRom, curveMonotoneX } from '@visx/curve';
import { MarkerCircle } from '@visx/marker';
import { GlyphCircle } from '@visx/glyph'

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
const CHART_RIGHT_MARGIN = 4; // so points are not cut off from the right
const CHART_TOP_MARGIN = 4 // so points are not cut off from the top

export const ValuesOverTimeAssertionResultChart = ({ data, timeRange, chartDimensions }: Props) => {
    const chartInnerWidth = chartDimensions.width - CHART_AXIS_LEFT_WIDTH - CHART_RIGHT_MARGIN
    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT - CHART_TOP_MARGIN

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
                <Group left={CHART_AXIS_LEFT_WIDTH} top={CHART_TOP_MARGIN}>
                    <LinePath
                        data={data.dataPoints}
                        x={(d) => xScale(d.time) ?? 0}
                        y={(d) => yScale(d.result.yValue ?? 0) ?? 0}
                        stroke={ACCENT_COLOR_HEX}
                        strokeWidth={2}
                    />

                    {data.dataPoints.map(dataPoint => {
                        // const barWidth = 8;
                        const xOffset = xScale(new Date(dataPoint.time));
                        const yOffset = yScale(dataPoint.result.yValue ?? 0);
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
                                    {/* <Bar
                                        key={`bar-${dataPoint.time}`}
                                        x={barX}
                                        y={yOffset}
                                        stroke="white"
                                        width={barWidth}
                                        height={chartInnerHeight - yOffset}
                                        fill={fillColor}
                                    /> */}
                                    <GlyphCircle
                                        left={xOffset}
                                        top={yOffset}
                                        fill={fillColor}
                                        stroke={'white'}
                                        size={48}
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
                        numTicks={generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs).length}
                        // tickValues={generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs)}
                        tickFormat={(v) => getCustomTimeScaleTickValue(v, timeRange)}
                    // tickLabelProps={(_) => ({
                    //     fill: ANTD_GRAY[9],
                    //     fontSize: 11,
                    //     textAnchor: 'middle',
                    // })}
                    />
                </Group>
            </svg>
        </>
    );
};
