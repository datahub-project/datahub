import React, { useMemo } from 'react';

import { Popover } from 'antd';
import { Bar } from '@visx/shape';
import { Group } from '@visx/group';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { Maybe } from 'graphql/jsutils/Maybe';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { Assertion, AssertionResultType } from '../../../../../../../../../../../../types.generated';
import { generateTimeScaleTickValues, getCustomTimeScaleTickValue, getFillColor } from './utils';
import { AssertionDataPoint, TimeRange } from '../types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import { scaleLinear } from 'd3-scale';

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


const CHART_LEFT_OFFSET_PX = 0;
const CHART_AXIS_BOTTOM_HEIGHT = 40;

/**
 * Assertion run results displayed on a horizontal timeline.
 */
export const TimeBasedAssertionResultChart = ({ data, timeRange, chartDimensions }: Props) => {

    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, chartDimensions.width],
            }),
        [timeRange, chartDimensions.width],
    );

    const yMax = useMemo(
        () => Math.max(...data.dataPoints.map(point => point.result.yValue ?? 0)),
        [data.dataPoints]
    )
    const yScale = useMemo(
        () =>
            scaleLinear(
                [0, yMax],
                [0, chartInnerHeight],
            ),
        [yMax]
    );

    return (
        <>
            <svg width={chartDimensions.width} height={chartDimensions.height}>
                <Group>
                    {data.dataPoints.map(dataPoint => {
                        const barWidth = 8;
                        const barX = xScale(new Date(dataPoint.time));
                        const barHeight = yScale(dataPoint.result.yValue ?? 0);
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
                                        y={chartInnerHeight - barHeight}
                                        stroke="white"
                                        width={barWidth}
                                        height={barHeight}
                                        fill={fillColor}
                                    />
                                </Popover>
                            </LinkWrapper>
                        );
                    })}
                </Group>

                <AxisBottom
                    top={chartInnerHeight}
                    left={CHART_LEFT_OFFSET_PX}
                    scale={xScale}
                    stroke={ANTD_GRAY[5]}
                    tickValues={generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs)}
                    tickFormat={(v) => getCustomTimeScaleTickValue(v, timeRange)}
                    tickStroke={ANTD_GRAY[5]}
                    tickLabelProps={(_) => ({
                        fontSize: 11,
                        angle: 0,
                        textAnchor: 'middle',
                    })}
                />
            </svg>
        </>
    );
};
