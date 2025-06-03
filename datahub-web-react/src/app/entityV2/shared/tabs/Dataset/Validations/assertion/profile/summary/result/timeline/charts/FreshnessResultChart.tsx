import { Popover } from '@components';
import { AxisBottom } from '@visx/axis';
import { LinearGradient } from '@visx/gradient';
import { GridColumns } from '@visx/grid';
import { Group } from '@visx/group';
import { scaleUtc } from '@visx/scale';
import { AreaClosed } from '@visx/shape';
import { scaleLinear } from 'd3-scale';
import React, { useMemo, useState } from 'react';

import { CandleStick } from '@app/dataviz/candle/CandleStick';
import { calculateOverlapBetweenTwoMarkers } from '@app/dataviz/utils';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { getAnomalyFeedbackContext } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionResultPopoverContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopoverContent';
import {
    AssertionDataPoint,
    AssertionResultChartData,
    TimeRange,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/types';
import {
    ACCENT_COLOR_HEX,
    generateTimeScaleTickValues,
    getCustomTimeScaleTickValue,
    getFillColor,
    getWindowStartAndEndDatesForFreshnessAssertionRun,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/utils';
import { tryGetActualUpdatedTimestampFromAssertionResult } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultExtractionUtils';
import { LinkWrapper } from '@app/shared/LinkWrapper';

import { AssertionResultType } from '@types';

type Props = {
    data: AssertionResultChartData;
    timeRange: TimeRange;
    chartDimensions: {
        width: number;
        height: number;
    };
    renderHeader?: (title?: string) => JSX.Element;
    refreshData?: () => Promise<unknown>;
};

const CHART_HORIZ_MARGIN = 36;
const CHART_AXIS_BOTTOM_HEIGHT = 40;
const CHART_AXIS_TOP_MARGIN = 24;

const PRIMARY_CANDLE_STICK_BAR_WIDTH = 5;

/**
 * Specifically for freshness assertions.
 */
export const FreshnessResultChart = ({ data, timeRange, chartDimensions, renderHeader, refreshData }: Props) => {
    const { dataPoints } = data;

    // ----------------- States and data calculations ----------------- //
    // Handle special visuals when user hovers over a data point
    const [mountedDataPoint, setMountedDataPoint] = useState<AssertionDataPoint>();
    const maybeMounteDataPointWindowRangeTicks: Date[] | undefined = useMemo(
        () => getWindowStartAndEndDatesForFreshnessAssertionRun(mountedDataPoint, dataPoints),
        [mountedDataPoint, dataPoints],
    );
    const maybeMountedDataPointFillColor = mountedDataPoint && getFillColor(mountedDataPoint.result.type);
    const maybeMountedDataPointDatasetUpdateDate: number | undefined = useMemo(() => {
        const result = mountedDataPoint?.relatedRunEvent?.result;
        if (!result || result.type === AssertionResultType.Error) return undefined;
        const maybeUpdatedTs = tryGetActualUpdatedTimestampFromAssertionResult(result);
        return maybeUpdatedTs;
    }, [mountedDataPoint]);

    const timeScaleTicks: Date[] = generateTimeScaleTickValues(timeRange.startMs, timeRange.endMs);

    // ----------------- Visual calculations ----------------- //
    const chartInnerHeight = chartDimensions.height - CHART_AXIS_BOTTOM_HEIGHT - CHART_AXIS_TOP_MARGIN;
    const chartInnerWidth = chartDimensions.width - CHART_HORIZ_MARGIN;

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, chartInnerWidth],
            }),
        [timeRange, chartInnerWidth],
    );

    // get x Axis for the black candle(TS Marker)
    const tsMarkerX = maybeMountedDataPointDatasetUpdateDate ? xScale(maybeMountedDataPointDatasetUpdateDate) : 0;

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
                            strokeLinecap: 'round',
                            strokeWidth: 1,
                            strokeDasharray: '1 4',
                        }}
                    />

                    {/* Expected window of mounted data point (currently being hovered) */}
                    {maybeMounteDataPointWindowRangeTicks
                        ? [
                              <GridColumns
                                  scale={xScale}
                                  tickValues={maybeMounteDataPointWindowRangeTicks}
                                  height={chartInnerHeight}
                                  lineStyle={{
                                      stroke: maybeMountedDataPointFillColor,
                                      strokeLinecap: 'round',
                                      strokeWidth: 1,
                                      strokeDasharray: '1 4',
                                  }}
                              />,
                              <LinearGradient
                                  id="area-gradient"
                                  from={maybeMountedDataPointFillColor}
                                  to={maybeMountedDataPointFillColor}
                                  fromOpacity={0.25}
                                  toOpacity={0.1}
                              />,
                              <AreaClosed
                                  data={maybeMounteDataPointWindowRangeTicks}
                                  x={xScale}
                                  y={1}
                                  yScale={scaleLinear([0, 1], [chartInnerHeight, 0])}
                                  strokeWidth={1}
                                  fill="url(#area-gradient)"
                              />,
                          ]
                        : null}

                    {/* Dataset updated TS marker (shows when you hover over a data point) */}
                    {maybeMountedDataPointDatasetUpdateDate ? (
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
                                },
                            }}
                            opacity={1}
                        />
                    ) : null}

                    {/* Candle data points */}
                    {dataPoints.map((dataPoint, i) => {
                        const xOffset = xScale(new Date(dataPoint.time));

                        // Check if this point overlaps with the last data point
                        let markerOverlapPx: number | undefined;
                        const maybePreviousDataPoint: AssertionDataPoint | undefined = dataPoints[i - 1];
                        if (maybePreviousDataPoint) {
                            const lastPointXOffset = xScale(new Date(maybePreviousDataPoint.time));
                            markerOverlapPx = calculateOverlapBetweenTwoMarkers(
                                {
                                    xOffset,
                                    width: PRIMARY_CANDLE_STICK_BAR_WIDTH,
                                },
                                {
                                    xOffset: lastPointXOffset,
                                    width: PRIMARY_CANDLE_STICK_BAR_WIDTH,
                                },
                            );
                        }

                        // setting offset as per the x axis of black and green candle
                        const opacity = xOffset - tsMarkerX <= PRIMARY_CANDLE_STICK_BAR_WIDTH ? 0.4 : 1;

                        const fillColor = getFillColor(dataPoint.result.type);

                        const { isMissedAlarm, isFalseAlarm } = getAnomalyFeedbackContext(
                            data.context.assertion,
                            dataPoint.relatedRunEvent,
                        );

                        return (
                            <CandleStick
                                showWarningOverlay={isFalseAlarm || isMissedAlarm}
                                candleHeight={chartInnerHeight - yOffset}
                                parentChartHeight={chartInnerHeight}
                                barWidth={PRIMARY_CANDLE_STICK_BAR_WIDTH}
                                shapeSize={150}
                                leftOffset={xOffset}
                                markerOverlapPx={markerOverlapPx}
                                shape={{ type: 'circle' }}
                                opacity={
                                    (mountedDataPoint && (mountedDataPoint.time === dataPoint.time ? opacity : 0.1)) ||
                                    1
                                }
                                color={fillColor}
                                wrapper={(children) => (
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
                                                    monitor={data.context.monitor}
                                                    run={dataPoint.relatedRunEvent}
                                                    refetchResults={refreshData}
                                                />
                                            }
                                            showArrow={false}
                                            onOpenChange={(visible) =>
                                                setMountedDataPoint(visible ? dataPoint : undefined)
                                            }
                                        >
                                            {children}
                                        </Popover>
                                    </LinkWrapper>
                                )}
                            />
                        );
                    })}
                </Group>
            </svg>
        </>
    );
};
