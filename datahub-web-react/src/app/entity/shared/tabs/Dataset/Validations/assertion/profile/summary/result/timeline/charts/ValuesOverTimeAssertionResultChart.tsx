import React, { useMemo } from 'react';

import _ from 'lodash';
import { Popover } from '@components';
import { AreaClosed, LinePath } from '@visx/shape';
import { Group } from '@visx/group';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { GlyphCircle } from '@visx/glyph';
import { LinearGradient } from '@visx/gradient';
import { scaleLinear } from 'd3-scale';

import { ANTD_GRAY } from '../../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../../shared/LinkWrapper';
import {
    ACCENT_COLOR_HEX,
    EXPECTED_RANGE_SHADE_COLOR,
    duplicateDataPointsAcrossBufferedTimeRange,
    generateTimeScaleTickValues,
    getCustomTimeScaleTickValue,
    getFillColor,
    tryGetUpperAndLowerYRangeFromAssertionRunEvent,
} from './utils';
import { AssertionDataPoint, AssertionResultChartData, TimeRange } from './types';
import { AssertionResultPopoverContent } from '../../../../shared/result/AssertionResultPopoverContent';
import {
    calculateOverlapBetweenTwoMarkers,
    calculateYScaleExtentForChart,
    truncateNumberForDisplay,
} from '../../../../../../../../../../../dataviz/utils';
import { getTimeRangeDisplay } from '../utils';
import { INTERVAL_TO_MS } from '../../../../../../../../../../../shared/time/timeUtils';
import { DateInterval } from '../../../../../../../../../../../../types.generated';

type Props = {
    data: AssertionResultChartData;
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    };
    renderHeader?: (title?: string) => JSX.Element;
};

const CHART_AXIS_LEFT_WIDTH = 32;
const CHART_AXIS_BOTTOM_HEIGHT = 40;
const CHART_RIGHT_MARGIN = 12;
const CHART_TOP_MARGIN = 8;

const NUM_TICKS_AXIS_LEFT = 3;

const PRIMARY_DATA_POINT_SIZE = 5;

export const ValuesOverTimeAssertionResultChart = ({ data, timeRange, chartDimensions, renderHeader }: Props) => {
    const rawDataPoints = data.dataPoints;

    const chartInnerWidth = chartDimensions.width - CHART_AXIS_LEFT_WIDTH - CHART_RIGHT_MARGIN;
    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT - CHART_TOP_MARGIN;

    // Results line extractors
    const getY = (point: AssertionDataPoint, options: { fallback: number } = { fallback: 0 }): number =>
        point.result.yValue ?? options.fallback;
    const getX = (point: AssertionDataPoint): number => point.time;

    // Expected line extractors
    const getExpectedYs = (point: AssertionDataPoint): { high?: number; low?: number } =>
        tryGetUpperAndLowerYRangeFromAssertionRunEvent(point.relatedRunEvent);

    // Define scales
    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, chartInnerWidth],
            }),
        [timeRange, chartInnerWidth],
    );

    const { yScale, actualResultsExtent } = useMemo(() => {
        const actualYValues = rawDataPoints
            .filter((point) => typeof point.result.yValue === 'number')
            .map((p) => getY(p));
        const expectedYValues: number[] = rawDataPoints.flatMap(
            (point) =>
                [getExpectedYs(point).high, getExpectedYs(point).low].filter(
                    (maybeNumber) => typeof maybeNumber === 'number',
                ) as number[],
        );

        const actualMin = actualYValues.length ? Math.min(...actualYValues) : 0;
        const actualMax = actualYValues.length ? Math.max(...actualYValues) : 0;
        const actualAverageValue = (actualMax + actualMin) / 2;

        const expectedMin = expectedYValues.length ? Math.min(...expectedYValues) : 0;
        const expectedMax = expectedYValues.length ? Math.max(...expectedYValues) : 0;

        const allYValues = actualYValues.concat(expectedYValues);
        const { min, max } = calculateYScaleExtentForChart(allYValues, {
            defaultYValue: 0,
            yScaleBufferFactor: 0.1,
        });

        return {
            yScale: scaleLinear([min, max], [chartInnerHeight, 0]),
            actualResultsExtent: {
                min: actualMin,
                max: actualMax,
                average: actualAverageValue,
            },
            expectedResultsExtent: expectedYValues.length
                ? {
                      min: expectedMin,
                      max: expectedMax,
                  }
                : undefined,
        };
    }, [rawDataPoints, chartInnerHeight]);

    // Coalesce the nullish yValues in the data points with defaults (ie. when the event maps to an 'initializing' state)
    const defaultYValue = actualResultsExtent.average;
    const dataPoints = rawDataPoints.map((dataPoint) => {
        // Doing a semi-shallow 2-level clone because the result data can have a lot of nesting
        const point = _.clone(dataPoint);
        point.result = _.clone(point.result);
        point.result.yValue = getY(dataPoint, {
            fallback: defaultYValue,
        });
        return point;
    });
    // If only 1 data point, add some buffer on the ts range so the line shows
    const expectedRangeDataPoints =
        dataPoints.length === 1
            ? duplicateDataPointsAcrossBufferedTimeRange(dataPoints[0], 2 * INTERVAL_TO_MS[DateInterval.Minute])
            : dataPoints;

    const distance = Math.max(...yScale.domain()) - Math.min(...yScale.domain());
    const skipRounding = distance <= 2;

    /* NOTE: the nodes in an svg that are first will have a lower z-index at paint-time */
    return (
        <>
            {renderHeader?.(data.yAxisLabel ? `${data.yAxisLabel} over time` : getTimeRangeDisplay(timeRange))}
            <svg width={chartDimensions.width} height={chartDimensions.height}>
                <Group left={CHART_AXIS_LEFT_WIDTH} top={CHART_TOP_MARGIN}>
                    {/* ----- Axis ----- */}
                    <AxisLeft
                        scale={yScale}
                        stroke={ANTD_GRAY[4]}
                        tickStroke={ANTD_GRAY[9]}
                        tickLength={4}
                        numTicks={NUM_TICKS_AXIS_LEFT}
                        tickFormat={(v) => truncateNumberForDisplay(v.valueOf(), skipRounding)}
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

                    {/* ----- Expected Results Lines ----- */}
                    {/* Min */}
                    <LinePath
                        data={expectedRangeDataPoints}
                        x={(d) => xScale(getX(d))}
                        // NOTE nullish 'low's should never show because the `defined` prop below removes them
                        y={(d) => yScale(getExpectedYs(d).low ?? 0)}
                        defined={(d) => typeof getExpectedYs(d).low === 'number'}
                        stroke={EXPECTED_RANGE_SHADE_COLOR}
                        strokeDasharray="4 4"
                        strokeWidth={1}
                    />
                    {/* Max */}
                    <LinePath
                        data={expectedRangeDataPoints}
                        x={(d) => xScale(getX(d)) ?? 0}
                        // NOTE nullish 'high's should never show because the `defined` prop below removes them
                        y={(d) => yScale(getExpectedYs(d).high ?? 0)}
                        defined={(d) => typeof getExpectedYs(d).high === 'number'}
                        stroke={EXPECTED_RANGE_SHADE_COLOR}
                        strokeDasharray="4 4"
                        strokeWidth={1}
                    />
                    {/* Shade expected range */}
                    <LinearGradient
                        id="expected-area-gradient"
                        from={EXPECTED_RANGE_SHADE_COLOR}
                        to={EXPECTED_RANGE_SHADE_COLOR}
                        fromOpacity={0.15}
                        toOpacity={0.1}
                    />
                    <AreaClosed
                        data={expectedRangeDataPoints}
                        x={(d) => xScale(getX(d))}
                        y0={(d) => yScale(getExpectedYs(d).low ?? yScale.domain()[0])} // first element of domain is the extent low
                        y1={(d) => yScale(getExpectedYs(d).high ?? yScale.domain()[1])} // second element of the domain is the extent high
                        defined={(d) => typeof (getExpectedYs(d).high ?? getExpectedYs(d).low) === 'number'}
                        yScale={yScale}
                        strokeWidth={1}
                        fill="url(#expected-area-gradient)"
                    />

                    {/* ----- Actual Results Line with gradient ----- */}
                    <LinearGradient
                        id="results-area-gradient"
                        from={ACCENT_COLOR_HEX}
                        to={ACCENT_COLOR_HEX}
                        fromOpacity={0.2}
                        toOpacity={0}
                    />
                    <AreaClosed
                        data={dataPoints}
                        x={(d) => xScale(getX(d))}
                        y={(d) => yScale(getY(d))}
                        yScale={yScale}
                        strokeWidth={1}
                        fill="url(#results-area-gradient)"
                    />
                    <LinePath
                        data={dataPoints}
                        x={(d) => xScale(getX(d))}
                        y={(d) => yScale(getY(d))}
                        stroke={ACCENT_COLOR_HEX}
                        strokeWidth={2}
                    />

                    {/* ----- Circular datapoints ----- */}
                    {dataPoints.map((dataPoint, i) => {
                        const xOffset = xScale(new Date(dataPoint.time));
                        const yOffset = yScale(getY(dataPoint, { fallback: defaultYValue }));

                        // Check if this point overlaps with the last data point
                        let markerOverlapPx: number | undefined;
                        const maybePreviousDataPoint: AssertionDataPoint | undefined = dataPoints[i - 1];
                        if (maybePreviousDataPoint) {
                            const lastPointXOffset = xScale(new Date(maybePreviousDataPoint.time));
                            markerOverlapPx = calculateOverlapBetweenTwoMarkers(
                                {
                                    xOffset,
                                    width: PRIMARY_DATA_POINT_SIZE,
                                },
                                {
                                    xOffset: lastPointXOffset,
                                    width: PRIMARY_DATA_POINT_SIZE,
                                },
                            );
                        }

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
                                    {/* TODO(jayacryl) make dots appear/animate when user hovers over */}
                                    <GlyphCircle
                                        left={xOffset}
                                        top={yOffset}
                                        fill={fillColor}
                                        stroke="white"
                                        strokeWidth={1}
                                        size={PRIMARY_DATA_POINT_SIZE * 20}
                                        filter={
                                            markerOverlapPx ? undefined : 'drop-shadow(0px 1px 2px rgb(0 0 0 / 0.2))'
                                        }
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
