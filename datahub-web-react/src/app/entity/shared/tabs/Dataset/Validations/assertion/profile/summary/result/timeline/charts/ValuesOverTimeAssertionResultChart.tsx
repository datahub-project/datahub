import React, { useMemo } from 'react';

import _ from 'lodash';
import { Popover } from 'antd';
import { AreaClosed, LinePath } from '@visx/shape';
import { Group } from '@visx/group';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { GlyphCircle } from '@visx/glyph'
import { LinearGradient } from '@visx/gradient'
import { scaleLinear } from 'd3-scale';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { ACCENT_COLOR_HEX, generateTimeScaleTickValues, getCustomTimeScaleTickValue, getFillColor } from './utils';
import { AssertionResultChartData, TimeRange } from './types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import { truncateNumberForDisplay } from '../../../../../../../../../../../dataviz/utils';

type Props = {
    data: AssertionResultChartData;
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    }
    renderHeader?: (title?: string) => JSX.Element
};


const CHART_AXIS_LEFT_WIDTH = 48;
const CHART_AXIS_BOTTOM_HEIGHT = 40;
const CHART_RIGHT_MARGIN = 2;
const CHART_TOP_MARGIN = 8;

export const ValuesOverTimeAssertionResultChart = ({ data, timeRange, chartDimensions, renderHeader }: Props) => {
    const rawDataPoints = data.dataPoints

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

    const { yScale, extent } = useMemo(
        () => {
            const yValues = rawDataPoints.filter(point => typeof point.result.yValue === 'number').map(point => point.result.yValue!)
            const realMin = (Math.min(...yValues) || 0)
            const realMax = (Math.max(...yValues) || 0)
            const averageValue = (realMax + realMin) / 2
            let min = realMin;
            let max = realMax;

            // Add some extra range above and below if the min and the max are the same so things are nicely centered
            if (realMin === realMax) {
                const averageValueBase = Math.floor(averageValue).toString().length
                const differentiator = 10 ** (averageValueBase - 1)
                min -= differentiator;
                max += differentiator;
            }

            return {
                yScale: scaleLinear(
                    [min, max],
                    [chartInnerHeight, 0],
                ),
                extent: {
                    min: realMin,
                    max: realMax,
                    average: averageValue,
                },
            }
        },
        [rawDataPoints, chartInnerHeight]
    );

    // Coalesce the nullish yValues in the data points with defaults
    const defaultYValue = extent.average
    const dataPoints = rawDataPoints.map(dataPoint => {
        // Doing a semi-shallow 2-level clone because the result data can have a lot of nesting
        const point = _.clone(dataPoint)
        point.result = _.clone(point.result)
        point.result.yValue = point.result.yValue ?? defaultYValue;
        return point
    })

    /* NOTE: the nodes in an svg that are first will have a lower z-index at paint-time */
    return <>
        {renderHeader?.(data.yAxisLabel && `${data.yAxisLabel} over time`)}
        <svg width={chartDimensions.width} height={chartDimensions.height}>
            <Group left={CHART_AXIS_LEFT_WIDTH} top={CHART_TOP_MARGIN}>
                {/* ----- Axis ----- */}
                <AxisLeft
                    scale={yScale}
                    stroke={ANTD_GRAY[4]}
                    tickStroke={ANTD_GRAY[9]}
                    tickLength={4}
                    numTicks={2}
                    tickFormat={v => truncateNumberForDisplay(v.valueOf())}
                    tickLabelProps={{
                        fill: ANTD_GRAY[9],
                        fontSize: 11,
                        textAnchor: 'end',
                    }}
                />
                <AxisBottom
                    top={chartInnerHeight}
                    scale={xScale}
                    stroke={ANTD_GRAY[4]}
                    tickStroke={ANTD_GRAY[9]}
                    tickLength={4}
                    tickValues={generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs)}
                    tickFormat={(v) => getCustomTimeScaleTickValue(v, timeRange)}
                />

                {/* ----- Line with gradient ----- */}
                <LinearGradient id="area-gradient" from={ACCENT_COLOR_HEX} to={ACCENT_COLOR_HEX} fromOpacity={0.25} toOpacity={0} />
                <AreaClosed
                    data={dataPoints}
                    x={(d) => xScale(d.time) ?? 0}
                    y={(d) => yScale(d.result.yValue ?? 0) ?? 0}
                    yScale={yScale}
                    strokeWidth={1}
                    fill="url(#area-gradient)"
                />

                <LinePath
                    data={dataPoints}
                    x={(d) => xScale(d.time) ?? 0}
                    y={(d) => yScale(d.result.yValue ?? 0) ?? 0}
                    stroke={ACCENT_COLOR_HEX}
                    strokeWidth={4}
                />

                {/* ----- Circular datapoints ----- */}
                {dataPoints.map(dataPoint => {
                    const xOffset = xScale(new Date(dataPoint.time));
                    const yOffset = yScale(dataPoint.result.yValue ?? defaultYValue);
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
                                {/* TODO(jayacryl) make dots appear/animate when user hovers over */}
                                <GlyphCircle
                                    left={xOffset}
                                    top={yOffset}
                                    fill={fillColor}
                                    stroke='white'
                                    strokeWidth={2}
                                    size={80}
                                    filter='drop-shadow(0px 1px 2px rgb(0 0 0 / 0.1))'
                                />
                            </Popover>
                        </LinkWrapper>
                    );
                })}
            </Group>
        </svg>
    </>;
};
