import React, { useMemo, useState } from 'react';

import { Popover } from 'antd';
import { Group } from '@visx/group';
import { AxisBottom } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { AreaClosed } from '@visx/shape';
import { GridColumns } from '@visx/grid'
import { LinearGradient } from '@visx/gradient';
import { scaleLinear } from 'd3-scale';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import { ACCENT_COLOR_HEX, generateTimeScaleTickValues, getCustomTimeScaleTickValue, getFillColor, getWindowStartAndEndDatesForFreshnessAssertionRun } from './utils';
import { AssertionDataPoint, AssertionResultChartData, TimeRange } from './types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import { tryGetActualUpdatedTimestampFromAssertionResult } from '../../../shared/resultExtractionUtils';
import { AssertionResultType } from '../../../../../../../../../../../../types.generated';
import { CandleStick } from '../../../../../../../../../../../dataviz/candle/CandleStick';

type Props = {
    data: AssertionResultChartData;
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    }
    renderHeader?: (title?: string) => JSX.Element
};


const CHART_HORIZ_MARGIN = 36;
const CHART_AXIS_BOTTOM_HEIGHT = 40;
const CHART_AXIS_TOP_MARGIN = 24;

/**
 * Specifically for freshness assertions.
 */
export const FreshnessResultChart = ({ data, timeRange, chartDimensions, renderHeader }: Props) => {
    const { dataPoints } = data;

    // ----------------- States and data calculations ----------------- //
    // Handle special visuals when user hovers over a data point
    const [mountedDataPoint, setMountedDataPoint] = useState<AssertionDataPoint>()
    const maybeMounteDataPointWindowRangeTicks: Date[] | undefined = useMemo(() =>
        getWindowStartAndEndDatesForFreshnessAssertionRun(mountedDataPoint, dataPoints),
        [mountedDataPoint, dataPoints]
    )
    const maybeMountedDataPointDatasetUpdateDate: number | undefined = useMemo(() => {
        const result = mountedDataPoint?.relatedRunEvent.result;
        if (!result || result.type === AssertionResultType.Error) return undefined;
        const maybeUpdatedTs = tryGetActualUpdatedTimestampFromAssertionResult(result);
        return maybeUpdatedTs;
    }, [mountedDataPoint])

    const timeScaleTicks: Date[] = generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs)

    // ----------------- Visual calculations ----------------- //
    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT - CHART_AXIS_TOP_MARGIN
    const chartInnerWidth = chartDimensions.width - CHART_HORIZ_MARGIN

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, chartInnerWidth],
            }),
        [timeRange, chartInnerWidth],
    );

    const yOffset = 0;
    return (
        <>
            {renderHeader?.(`${data.yAxisLabel ?? 'Freshness checks'} over time`)}
            <svg width={chartDimensions.width} height={chartDimensions.height}>
                <Group left={CHART_HORIZ_MARGIN / 2} top={CHART_AXIS_TOP_MARGIN}>
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
                            strokeLinecap: "round",
                            strokeWidth: 1,
                            strokeDasharray: '1 4'
                        }}
                    />

                    {/* Expected window of mounted data point (currently being hovered) */}
                    {maybeMounteDataPointWindowRangeTicks ? [
                        <GridColumns
                            scale={xScale}
                            tickValues={maybeMounteDataPointWindowRangeTicks}
                            height={chartInnerHeight}
                            lineStyle={{
                                stroke: ACCENT_COLOR_HEX,
                                strokeLinecap: "round",
                                strokeWidth: 1,
                                strokeDasharray: '1 4'
                            }}
                        />,
                        <LinearGradient id="area-gradient" from={ACCENT_COLOR_HEX} to={ACCENT_COLOR_HEX} fromOpacity={0.25} toOpacity={0.1} />,
                        <AreaClosed
                            data={maybeMounteDataPointWindowRangeTicks}
                            x={xScale}
                            y={1}
                            yScale={scaleLinear(
                                [0, 1],
                                [chartInnerHeight, 0]
                            )}
                            strokeWidth={1}
                            fill="url(#area-gradient)"
                        />,
                    ] : null}

                    {/* Dataset updated TS marker (shows when you hover over a data point) */}
                    {maybeMountedDataPointDatasetUpdateDate ?
                        <CandleStick
                            candleHeight={chartInnerHeight - yOffset}
                            parentChartHeight={chartInnerHeight}
                            barWidth={4}
                            shapeSize={120}
                            leftOffset={xScale(maybeMountedDataPointDatasetUpdateDate)}
                            color={ACCENT_COLOR_HEX}
                            shape={{
                                type: 'diamond',
                                extraProps: {
                                    strokeWidth: 1,
                                }
                            }}
                        />
                        : null}

                    {/* Candle data points */}
                    {dataPoints.map(dataPoint => {
                        const xOffset = xScale(new Date(dataPoint.time));
                        const fillColor = getFillColor(dataPoint.result.type);
                        return (
                            <CandleStick
                                candleHeight={chartInnerHeight - yOffset}
                                parentChartHeight={chartInnerHeight}
                                barWidth={5}
                                shapeSize={150}
                                leftOffset={xOffset}
                                shape={{ type: 'circle' }}
                                opacity={(mountedDataPoint && (mountedDataPoint.time === dataPoint.time ? 1 : 0.1)) || 1}
                                color={fillColor}
                                wrapper={(children) => <LinkWrapper key={dataPoint.time} to={dataPoint.result.resultUrl} target="_blank">
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
                                        onOpenChange={visible => setMountedDataPoint(visible ? dataPoint : undefined)}
                                    >{children}</Popover>
                                </LinkWrapper>}
                            />
                        );
                    })}

                </Group>
            </svg>
        </>
    );
};
