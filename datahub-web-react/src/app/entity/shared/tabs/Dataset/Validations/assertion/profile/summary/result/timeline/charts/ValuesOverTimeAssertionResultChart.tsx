import React, { useMemo } from 'react';

import { Popover } from 'antd';
import { LinePath } from '@visx/shape';
import { Group } from '@visx/group';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { GlyphCircle } from '@visx/glyph'
import { scaleLinear } from 'd3-scale';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { Assertion } from '../../../../../../../../../../../../types.generated';
import { ACCENT_COLOR_HEX, generateTimeScaleTickValues, getCustomTimeScaleTickValue, getFillColor } from './utils';
import { AssertionDataPoint, TimeRange } from './types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import { truncateNumberForDisplay } from '../../../../../../../../../../../dataviz/utils';

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
const CHART_RIGHT_MARGIN = 0; // so points are not cut off from the right
const CHART_TOP_MARGIN = 8 // so points are not cut off from the top

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

    const yScale = useMemo(
        () => {
            const yValues = data.dataPoints.filter(point => typeof point.result.yValue === 'number').map(point => point.result.yValue ?? 0)
            let min = (Math.min(...yValues) || 0)
            let max = (Math.max(...yValues) || 0)

            // Add some extra range above and below if the min and the max are the same so things are nicely centered
            if (min === max) {
                const averageValue = min + max / 2
                const averageValueBase = Math.floor(averageValue).toString().length
                const differentiator = 10 ** (averageValueBase - 1)
                min -= differentiator;
                max += differentiator;
            }

            return scaleLinear(
                [min, max],
                [chartInnerHeight, 0],
            )
        },
        [data.dataPoints, chartInnerHeight]
    );

    /* NOTE: the nodes in an svg that are first will have a lower z-index at paint-time */
    return <svg width={chartDimensions.width} height={chartDimensions.height}>
        <Group left={CHART_AXIS_LEFT_WIDTH} top={CHART_TOP_MARGIN}>
            <AxisLeft
                scale={yScale}
                stroke={ANTD_GRAY[5]}
                tickStroke={ANTD_GRAY[5]}
                numTicks={2}
                tickFormat={v => truncateNumberForDisplay(v.valueOf())}
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
            />

            <LinePath
                data={data.dataPoints}
                x={(d) => xScale(d.time) ?? 0}
                y={(d) => yScale(d.result.yValue ?? 0) ?? 0}
                stroke={ACCENT_COLOR_HEX}
                strokeWidth={4}
            />

            {data.dataPoints.map(dataPoint => {
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
                            <GlyphCircle
                                left={xOffset}
                                top={yOffset}
                                fill={fillColor}
                                stroke={'white'}
                                strokeWidth={2}
                                size={60}
                                filter='drop-shadow(0px 1px 2px rgb(0 0 0 / 0.2))'
                            />
                        </Popover>
                    </LinkWrapper>
                );
            })}
        </Group>
    </svg>;
};
