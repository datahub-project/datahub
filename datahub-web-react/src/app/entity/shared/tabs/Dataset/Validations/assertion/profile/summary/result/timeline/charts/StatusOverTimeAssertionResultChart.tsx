import React, { useMemo } from 'react';

import { Popover } from 'antd';
import { Bar } from '@visx/shape';
import { Group } from '@visx/group';
import { AxisBottom } from '@visx/axis';
import { scaleUtc } from '@visx/scale';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { generateTimeScaleTickValues, getCustomTimeScaleTickValue, getFillColor } from './utils';
import { AssertionResultChartData, TimeRange } from './types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';

type Props = {
    data: AssertionResultChartData;
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    }
};


const CHART_HORIZ_MARGIN = 16;
const CHART_AXIS_BOTTOM_HEIGHT = 40;

/**
 * Assertion run result status displayed on a horizontal timeline.
 * TODO(jayacryl) refactor to a pretty timeline line-view
 */
export const StatusOverTimeAssertionResultChart = ({ data, timeRange, chartDimensions }: Props) => {

    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT
    const chartInnerWidth = chartDimensions.width - CHART_HORIZ_MARGIN

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, chartInnerWidth],
            }),
        [timeRange, chartInnerWidth],
    );


    return (
        <>
            <svg width={chartDimensions.width} height={chartDimensions.height}>
                <Group left={CHART_HORIZ_MARGIN / 2}>
                    {data.dataPoints.map(dataPoint => {
                        const barWidth = 8;
                        const barX = xScale(new Date(dataPoint.time));
                        const barHeight = chartDimensions.height / 3;
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

                    <AxisBottom
                        top={chartInnerHeight}
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
                </Group>
            </svg>
        </>
    );
};
