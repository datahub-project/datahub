import { Popover, Text, colors } from '@components';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { GlyphCircle } from '@visx/glyph';
import { LinearGradient } from '@visx/gradient';
import { Group } from '@visx/group';
import { scaleUtc } from '@visx/scale';
import { AreaClosed, LinePath } from '@visx/shape';
import { scaleLinear } from 'd3-scale';
import _ from 'lodash';
import React, { useMemo, useState } from 'react';
import styled, { css } from 'styled-components';

import {
    calculateOverlapBetweenTwoMarkers,
    calculateYScaleExtentForChart,
    truncateNumberForDisplay,
} from '@app/dataviz/utils';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { getAnomalyFeedbackContext } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionResultPopoverContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopoverContent';
import AssertionChartHeader from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/AssertionChartHeader';
import {
    AssertionDataPoint,
    AssertionResultChartData,
    TimeRange,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/types';
import {
    ACCENT_COLOR_HEX,
    EXPECTED_RANGE_SHADE_COLOR,
    SelectionState,
    duplicateDataPointsAcrossBufferedTimeRange,
    generateTimeScaleTickValues,
    getCustomTimeScaleTickValue,
    getFillColor,
    tryGetUpperAndLowerYRangeFromAssertionRunEvent,
    useChartDragHandlers,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/utils';
import { getTimeRangeDisplay } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import { LinkWrapper } from '@app/shared/LinkWrapper';
import { INTERVAL_TO_MS } from '@app/shared/time/timeUtils';
import { useAppConfig } from '@app/useAppConfig';
import { Tooltip } from '@src/alchemy-components';

import { AssertionExclusionWindow, AssertionExclusionWindowType, DateInterval } from '@types';

type Props = {
    data: AssertionResultChartData;
    // only supports fixedRange exclusion windows for now
    exclusionWindows?: AssertionExclusionWindow[];
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    };
    onOpenTunePredictionsModal: () => void;
    refreshData?: () => Promise<unknown>;
    openAssertionNote?: () => void;
    onTimeRangeChange?: (startTimeMs: number, endTimeMs: number) => void;
};

const CHART_AXIS_LEFT_WIDTH = 32;
const CHART_AXIS_BOTTOM_HEIGHT = 40;
const CHART_RIGHT_MARGIN = 12;
const CHART_TOP_MARGIN = 8;

const NUM_TICKS_AXIS_LEFT = 3;

const PRIMARY_DATA_POINT_SIZE = 5;

const formatYAxisValue = (value: number, skipRounding: boolean) => {
    if (value >= 1000) {
        return truncateNumberForDisplay(value.valueOf(), skipRounding);
    }
    return Math.round((value + Number.EPSILON) * 100) / 100;
};

const ChartInteractionContainer = styled.div<{ hasSelection: boolean }>`
    position: relative;
    ${(props) =>
        props.hasSelection &&
        css`
            cursor: crosshair;
            user-select: none;
        `}

    /* Override cursor for interactive elements */
    .interactive-element {
        cursor: pointer !important;
    }
`;

const SelectionRectangle = styled.div<{
    startX: number;
    endX: number;
    height: number;
    marginTop: number;
    marginBottom: number;
}>`
    position: absolute;
    left: ${(props) => Math.min(props.startX, props.endX)}px;
    top: ${(props) => props.marginTop}px;
    width: ${(props) => Math.abs(props.endX - props.startX)}px;
    height: ${(props) => props.height - props.marginTop - props.marginBottom}px;
    background-color: ${colors.primary[200]};
    opacity: 0.1;
    border: 1px solid ${colors.violet[500]};
    pointer-events: none;
`;

export const ValuesOverTimeAssertionResultChart = ({
    data,
    exclusionWindows,
    timeRange,
    chartDimensions,
    refreshData,
    openAssertionNote,
    onTimeRangeChange,
    onOpenTunePredictionsModal,
}: Props) => {
    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;
    const rawDataPoints = data.dataPoints;

    const chartInnerWidth = chartDimensions.width - CHART_AXIS_LEFT_WIDTH - CHART_RIGHT_MARGIN;
    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT - CHART_TOP_MARGIN;

    // Selection state for drag functionality
    const [selectionState, setSelectionState] = useState<SelectionState>({
        isSelecting: false,
        selectionStart: null,
        selectionEnd: null,
    });

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

    // Mouse event handlers for drag selection
    const { handleMouseDown, handleMouseMove, handleMouseUp } = useChartDragHandlers(
        selectionState,
        setSelectionState,
        onTimeRangeChange,
        xScale,
        chartInnerWidth,
        CHART_AXIS_LEFT_WIDTH,
    );

    /* NOTE: the nodes in an svg that are first will have a lower z-index at paint-time */
    return (
        <>
            <AssertionChartHeader
                title={data.yAxisLabel ? `${data.yAxisLabel} over time` : getTimeRangeDisplay(timeRange)}
                timeRange={timeRange}
                assertion={data.context.assertion}
                monitor={data.context.monitor}
                onOpenTunePredictionsModal={onOpenTunePredictionsModal}
            />
            <ChartInteractionContainer
                hasSelection={!!onTimeRangeChange}
                onMouseDown={onTimeRangeChange ? handleMouseDown : undefined}
                onMouseMove={onTimeRangeChange ? handleMouseMove : undefined}
                onMouseUp={onTimeRangeChange ? handleMouseUp : undefined}
            >
                <svg width={chartDimensions.width} height={chartDimensions.height}>
                    <Group left={CHART_AXIS_LEFT_WIDTH} top={CHART_TOP_MARGIN}>
                        {/* ----- Axis ----- */}
                        <AxisLeft
                            scale={yScale}
                            stroke={ANTD_GRAY[4]}
                            tickStroke={ANTD_GRAY[9]}
                            tickLength={4}
                            numTicks={NUM_TICKS_AXIS_LEFT}
                            tickFormat={(v) => formatYAxisValue(v.valueOf(), skipRounding).toString()}
                            tickLabelProps={(value, _index) => {
                                return {
                                    fill: ANTD_GRAY[9],
                                    fontSize: 11,
                                    textAnchor: 'start' as const, // Explicitly cast to allowed type
                                    dx: '-2.5em',
                                    dy:
                                        formatYAxisValue(value.valueOf(), skipRounding).toString().length > 4
                                            ? '-0.3em'
                                            : '0.3em',
                                };
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

                        {/* ----- Exclusion Windows ----- */}
                        {exclusionWindows?.map((exclusionWindow) => {
                            // Only render FIXED_RANGE windows for now
                            if (
                                exclusionWindow.type !== AssertionExclusionWindowType.FixedRange ||
                                !exclusionWindow.fixedRange
                            ) {
                                return null;
                            }

                            const startTime = exclusionWindow.fixedRange.startTimeMillis;
                            const endTime = exclusionWindow.fixedRange.endTimeMillis;

                            // Calculate positions using the time scale
                            const startX = xScale(new Date(startTime));
                            const endX = xScale(new Date(endTime));

                            // Clamp to chart boundaries
                            const clampedStartX = Math.max(0, startX);
                            const clampedEndX = Math.min(chartInnerWidth, endX);
                            const width = clampedEndX - clampedStartX;

                            // Skip if window is completely outside the visible range
                            if (width <= 0) {
                                return null;
                            }

                            return (
                                <Tooltip
                                    title={
                                        <Text>
                                            <Text weight="bold" color="gray" colorLevel={600}>
                                                Training Data Exclusion Window
                                            </Text>
                                            <Text size="sm" weight="semiBold">
                                                &quot;
                                                {exclusionWindow.displayName || 'AI Model will ignore this time range.'}
                                                &quot;
                                            </Text>
                                            <Text size="sm">You can change this in the Settings tab.</Text>
                                        </Text>
                                    }
                                >
                                    <Group className="interactive-element">
                                        <rect
                                            x={clampedStartX}
                                            y={0}
                                            width={width}
                                            height={chartInnerHeight}
                                            fill={colors.red[700]}
                                            fillOpacity={0.15}
                                            style={{ cursor: 'pointer' }}
                                        />
                                    </Group>
                                </Tooltip>
                            );
                        })}

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

                            const { isMissedAlarm, isFalseAlarm } = getAnomalyFeedbackContext(
                                data.context.assertion,
                                dataPoint.relatedRunEvent,
                                onlineSmartAssertionsEnabled,
                            );

                            return (
                                <LinkWrapper
                                    key={dataPoint.time}
                                    to={dataPoint.result.resultUrl}
                                    target="_blank"
                                    className="interactive-element"
                                >
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
                                                monitor={data.context.monitor}
                                                run={dataPoint.relatedRunEvent}
                                                refetchResults={refreshData}
                                                openAssertionNote={openAssertionNote}
                                            />
                                        }
                                        showArrow={false}
                                    >
                                        <Group>
                                            <GlyphCircle
                                                left={xOffset}
                                                top={yOffset}
                                                fill={fillColor}
                                                stroke="white"
                                                strokeWidth={1}
                                                size={PRIMARY_DATA_POINT_SIZE * 20}
                                                filter={
                                                    markerOverlapPx
                                                        ? undefined
                                                        : 'drop-shadow(0px 1px 2px rgb(0 0 0 / 0.2))'
                                                }
                                            />
                                            {/* For Smart Assertions: '!' icon overlaps if this has been marked as a false positive or false negative */}
                                            {isFalseAlarm || isMissedAlarm ? (
                                                // // Downloaded from phosphor icons
                                                <svg
                                                    x={xOffset - PRIMARY_DATA_POINT_SIZE * 1}
                                                    y={yOffset - PRIMARY_DATA_POINT_SIZE * 1}
                                                    width={PRIMARY_DATA_POINT_SIZE * 2}
                                                    height={PRIMARY_DATA_POINT_SIZE * 2}
                                                    fill="white"
                                                    viewBox="0 0 256 256"
                                                >
                                                    <path d="M144,200a16,16,0,1,1-16-16A16,16,0,0,1,144,200Zm-16-40a8,8,0,0,0,8-8V48a8,8,0,0,0-16,0V152A8,8,0,0,0,128,160Z" />
                                                </svg>
                                            ) : null}
                                        </Group>
                                    </Popover>
                                </LinkWrapper>
                            );
                        })}
                    </Group>
                </svg>

                {/* Selection rectangle during drag */}
                {selectionState.selectionStart && selectionState.selectionEnd && (
                    <SelectionRectangle
                        startX={selectionState.selectionStart.x}
                        endX={selectionState.selectionEnd.x}
                        height={chartDimensions.height}
                        marginTop={CHART_TOP_MARGIN}
                        marginBottom={CHART_AXIS_BOTTOM_HEIGHT}
                    />
                )}
            </ChartInteractionContainer>
        </>
    );
};
