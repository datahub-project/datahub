import React, { useMemo } from 'react';

import { Popover } from '@components';
import { Group } from '@visx/group';
import { AxisBottom } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { GlyphCircle } from '@visx/glyph';
import { LinePath } from '@visx/shape';
import { GridColumns } from '@visx/grid';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { ACCENT_COLOR_HEX, generateTimeScaleTickValues, getCustomTimeScaleTickValue, getFillColor } from './utils';
import { AssertionResultChartData, TimeRange } from './types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import { getTimeRangeDisplay } from '../utils';

type Props = {
    data: AssertionResultChartData;
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    };
    renderHeader?: (title?: string) => JSX.Element;
};

const CHART_HORIZ_MARGIN = 36;
const CHART_AXIS_BOTTOM_HEIGHT = 40;

/**
 * Assertion run result status displayed on a horizontal timeline.
 * TODO(jayacryl) refactor to a pretty timeline line-view
 */
export const StatusOverTimeAssertionResultChart = ({ data, timeRange, chartDimensions, renderHeader }: Props) => {
    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT;
    const chartInnerWidth = chartDimensions.width - CHART_HORIZ_MARGIN;

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, chartInnerWidth],
            }),
        [timeRange, chartInnerWidth],
    );

    const timeScaleTicks = generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs);
    return (
        <>
            {renderHeader?.(getTimeRangeDisplay(timeRange))}
            <svg width={chartDimensions.width} height={chartDimensions.height}>
                <Group left={CHART_HORIZ_MARGIN / 2}>
                    {/* Axis */}
                    <AxisBottom
                        top={chartInnerHeight}
                        scale={xScale}
                        stroke={ANTD_GRAY[4]}
                        tickValues={timeScaleTicks}
                        tickFormat={(v) => getCustomTimeScaleTickValue(v, timeRange)}
                        tickStroke={ANTD_GRAY[9]}
                        tickLength={4}
                        tickLabelProps={{
                            fontSize: 11,
                            angle: 0,
                            textAnchor: 'middle',
                        }}
                    />

                    {/* Grid */}
                    <GridColumns
                        scale={xScale}
                        tickValues={timeScaleTicks}
                        height={chartInnerHeight}
                        lineStyle={{
                            stroke: ANTD_GRAY[5],
                            strokeLinecap: 'round',
                            strokeWidth: 1,
                            strokeDasharray: '1 4',
                        }}
                    />

                    {/* Line */}
                    <LinePath
                        // Full across the entire timeline
                        data={[0, chartInnerWidth]}
                        x={(x) => x}
                        y={chartDimensions.height / 3}
                        stroke={ACCENT_COLOR_HEX}
                        strokeWidth={0.5}
                    />

                    {/* Circular data points */}
                    {data.dataPoints.map((dataPoint) => {
                        const xOffset = xScale(new Date(dataPoint.time));
                        const yOffset = chartDimensions.height / 3;
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
                                    content={
                                        <AssertionResultPopoverContent
                                            assertion={data.context.assertion}
                                            run={dataPoint.relatedRunEvent}
                                        />
                                    }
                                    showArrow={false}
                                >
                                    <GlyphCircle
                                        left={xOffset}
                                        top={yOffset}
                                        fill={fillColor}
                                        stroke="white"
                                        strokeWidth={2}
                                        size={100}
                                        filter="drop-shadow(0px 1px 2.5px rgb(0 0 0 / 0.05))"
                                    />
                                </Popover>
                            </LinkWrapper>
                        );
                    })}
                </Group>
            </svg>
        </>
    );
};
