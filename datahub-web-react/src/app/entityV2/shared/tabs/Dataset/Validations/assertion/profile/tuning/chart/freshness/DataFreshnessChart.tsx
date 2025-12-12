import { Button, Text, colors } from '@components';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { GridColumns } from '@visx/grid';
import { Group } from '@visx/group';
import { scaleBand } from '@visx/scale';
import { Bar } from '@visx/shape';
import { TooltipWithBounds, useTooltip } from '@visx/tooltip';
import { scaleLinear } from 'd3-scale';
import moment from 'moment';
import { ArrowCounterClockwise } from 'phosphor-react';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import Loading from '@app/shared/Loading';

import { Operation } from '@types';

const BAR_COLOR = '#533FD1';
const BAR_EXCEEDS_MAX_COLOR = '#C4360B';

const CLIP_INDICATOR_HEIGHT = 7;

// Cadence categorization configuration, helps normalize the Y-axis values
const CADENCE_CONFIG = {
    hourly: {
        minHours: 0, // Minimum average time between updates for hourly category
        maxHours: 12, // Maximum average time between updates for hourly category
        maxYMs: 12 * 60 * 60 * 1000, // Maximum Y-axis value in milliseconds (12 hours) before we clip the bar
        useDaysFormat: false,
        stepSizeMs: 4 * 60 * 60 * 1000, // Step size for Y-axis ticks (4 hours)
    },
    daily: {
        minHours: 12,
        maxHours: 48, // (48 hours)
        maxYMs: 36 * 60 * 60 * 1000, // (36 hours)
        useDaysFormat: false,
        stepSizeMs: 4 * 60 * 60 * 1000, // Step size for Y-axis ticks (4 hours)
    },
    weekly: {
        minHours: 48,
        maxHours: Infinity,
        maxYMs: 14 * 24 * 60 * 60 * 1000, // (14 days)
        useDaysFormat: true, // Show in days on the Y-axis for large values
        stepSizeMs: 2 * 24 * 60 * 60 * 1000, // Step size for Y-axis ticks (2 days)
    },
} as const;

const Legend = styled.div`
    display: flex;
    gap: 16px;
    margin-bottom: 16px;
    font-size: 12px;
`;

const LegendItem = styled(Text)<{ $color: string }>`
    display: flex;
    align-items: center;
    gap: 4px;
    color: ${colors.gray[1700]};

    &::before {
        content: '';
        width: 12px;
        height: 12px;
        border-radius: 100%;
        background-color: ${(props) => props.$color};
        display: block;
    }
`;

const LegendControlsContainer = styled.div`
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ResetZoomButton = styled(Button)`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const ZoomInstructionText = styled(Text)`
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

const ChartContainer = styled.div`
    display: flex;
    flex-direction: column;
    position: relative;
`;

const ChartInteractionContainer = styled.div<{ $isSelecting: boolean }>`
    position: relative;
    ${(props) =>
        props.$isSelecting &&
        `
        cursor: crosshair;
    `}
    /* Ensure tooltips render above other content */
    z-index: 1;
`;

const SelectionRectangle = styled.div<{
    $startX: number;
    $endX: number;
    $height: number;
    $marginTop: number;
    $marginBottom: number;
}>`
    position: absolute;
    left: ${(props) => Math.min(props.$startX, props.$endX)}px;
    top: ${(props) => props.$marginTop}px;
    width: ${(props) => Math.abs(props.$endX - props.$startX)}px;
    height: ${(props) => props.$height - props.$marginTop - props.$marginBottom}px;
    background-color: ${colors.primary[200]};
    opacity: 0.1;
    border: 1px solid ${BAR_COLOR};
    pointer-events: none;
`;

const CHART_HORIZ_MARGIN = 36;
const CHART_AXIS_BOTTOM_HEIGHT = 40;
const CHART_AXIS_TOP_MARGIN = 24;
const CHART_AXIS_LEFT_WIDTH = 20;

type Props = {
    operations: Operation[];
    loading: boolean;
    width: number;
    height: number;
    onRangeChange: (range: { start: number; end: number }) => void;
    resetRange?: () => void;
    currentTime: number;
    range?: { start: number; end: number };
};

/**
 * Formats time duration in milliseconds to a human-readable string
 */
const formatTimeDuration = (ms: number, useDays = false): string => {
    const seconds = ms / 1000;
    const minutes = seconds / 60;
    const hours = minutes / 60;
    const days = hours / 24;

    if (useDays) {
        return `${Math.round(days)}d`;
    }
    // When useDays is false, always show hours (even if >= 24 hours)
    // This prevents duplicate labels when values round to the same day
    if (hours >= 1) {
        return `${Math.round(hours)}h`;
    }
    if (minutes >= 1) {
        return `${Math.round(minutes)}m`;
    }
    return `${Math.round(seconds)}s`;
};

type CadenceCategory = 'hourly' | 'daily' | 'weekly';

/**
 * Determines the cadence category based on average time between updates
 */
const determineCadence = (
    barData: Array<{ timeSinceLastUpdate: number }>,
): {
    category: CadenceCategory;
    maxY: number; // in milliseconds
    useDaysFormat: boolean;
    stepSizeMs: number; // Step size for Y-axis ticks
} => {
    // Helper function to round up to nearest multiple of step size
    const roundToStep = (value: number, step: number): number => {
        return Math.ceil(value / step) * step;
    };

    if (barData.length === 0) {
        return {
            category: 'daily',
            maxY: CADENCE_CONFIG.daily.maxYMs,
            useDaysFormat: CADENCE_CONFIG.daily.useDaysFormat,
            stepSizeMs: CADENCE_CONFIG.daily.stepSizeMs,
        };
    }

    // Calculate average time between updates (excluding the first point which has 0)
    const validDiffs = barData.map((d) => d.timeSinceLastUpdate).filter((diff) => diff > 0);

    if (validDiffs.length === 0) {
        return {
            category: 'daily',
            maxY: CADENCE_CONFIG.daily.maxYMs,
            useDaysFormat: CADENCE_CONFIG.daily.useDaysFormat,
            stepSizeMs: CADENCE_CONFIG.daily.stepSizeMs,
        };
    }

    const averageMs = validDiffs.reduce((sum, diff) => sum + diff, 0) / validDiffs.length;
    const averageHours = averageMs / (60 * 60 * 1000);
    const maxFreshnessDelay = Math.max(...barData.map((d) => d.timeSinceLastUpdate));

    // Hourly: average >= minHours and < maxHours
    if (averageHours >= CADENCE_CONFIG.hourly.minHours && averageHours < CADENCE_CONFIG.hourly.maxHours) {
        const rawMaxY = Math.min(CADENCE_CONFIG.hourly.maxYMs, maxFreshnessDelay);
        const maxY = roundToStep(rawMaxY, CADENCE_CONFIG.hourly.stepSizeMs);
        return {
            category: 'hourly',
            maxY,
            useDaysFormat: CADENCE_CONFIG.hourly.useDaysFormat,
            stepSizeMs: CADENCE_CONFIG.hourly.stepSizeMs,
        };
    }

    // Daily: average >= minHours and <= maxHours
    if (averageHours >= CADENCE_CONFIG.daily.minHours && averageHours <= CADENCE_CONFIG.daily.maxHours) {
        const rawMaxY = Math.min(CADENCE_CONFIG.daily.maxYMs, maxFreshnessDelay);
        const maxY = roundToStep(rawMaxY, CADENCE_CONFIG.daily.stepSizeMs);
        return {
            category: 'daily',
            maxY,
            useDaysFormat: CADENCE_CONFIG.daily.useDaysFormat,
            stepSizeMs: CADENCE_CONFIG.daily.stepSizeMs,
        };
    }

    // Weekly: average >= minHours
    const rawMaxY = Math.min(CADENCE_CONFIG.weekly.maxYMs, maxFreshnessDelay);
    const maxY = roundToStep(rawMaxY, CADENCE_CONFIG.weekly.stepSizeMs);
    return {
        category: 'weekly',
        maxY,
        useDaysFormat: CADENCE_CONFIG.weekly.useDaysFormat,
        stepSizeMs: CADENCE_CONFIG.weekly.stepSizeMs,
    };
};

/**
 * Calculates bar data for individual operations ordered by time
 * Each operation gets a bar showing time since the previous operation
 */
const calculateBarData = (
    operations: Operation[],
    startMs: number,
    endMs: number,
): Array<{
    timestampMillis: number;
    timeSinceLastUpdate: number;
    updateTime: string;
}> => {
    if (operations.length === 0) {
        return [];
    }

    // Sort operations by lastUpdatedTimestamp
    const sortedOperations = [...operations].sort((a, b) => a.lastUpdatedTimestamp - b.lastUpdatedTimestamp);

    // Filter operations within the time range
    const filteredOperations = sortedOperations.filter(
        (operation) => operation.lastUpdatedTimestamp >= startMs && operation.lastUpdatedTimestamp <= endMs,
    );

    if (filteredOperations.length === 0) {
        return [];
    }

    // Calculate time since last update for each operation
    const barData: Array<{
        timestampMillis: number;
        timeSinceLastUpdate: number;
        updateTime: string;
    }> = [];

    // Find the previous operation before the first operation in range (if any)
    // We use this to calculate the time since the last update for the first operation in the range
    const firstOperation = filteredOperations[0];
    const firstOperationIndex = sortedOperations.findIndex(
        (op) => op.lastUpdatedTimestamp === firstOperation.lastUpdatedTimestamp,
    );
    const previousOperationIndex = firstOperationIndex - 1;
    const previousOperation = previousOperationIndex >= 0 ? sortedOperations[previousOperationIndex] : null;
    const lastOperationTimestamp = previousOperation ? previousOperation.lastUpdatedTimestamp : null;

    for (let i = 0; i < filteredOperations.length; i++) {
        const operation = filteredOperations[i];
        let timeSinceLastUpdate = 0;

        if (i === 0 && lastOperationTimestamp !== null) {
            // First operation in range: calculate from previous operation
            timeSinceLastUpdate = operation.lastUpdatedTimestamp - lastOperationTimestamp;
        } else if (i > 0) {
            // Calculate from previous operation in range
            timeSinceLastUpdate = operation.lastUpdatedTimestamp - filteredOperations[i - 1].lastUpdatedTimestamp;
        }
        if (timeSinceLastUpdate > 0) {
            barData.push({
                timestampMillis: operation.lastUpdatedTimestamp,
                timeSinceLastUpdate,
                updateTime: moment(operation.lastUpdatedTimestamp).format('MMM DD, YYYY HH:mm:ss'),
            });
        }
    }

    return barData;
};

export const DataFreshnessChart = ({
    operations,
    loading,
    width,
    height,
    onRangeChange,
    resetRange,
    currentTime,
    range,
}: Props) => {
    // Calculate time range from range prop if provided, otherwise from operations data
    const timeRange = (() => {
        if (range) {
            return {
                startMs: range.start,
                endMs: range.end,
            };
        }
        if (operations.length === 0) {
            return {
                startMs: currentTime - 7 * 24 * 60 * 60 * 1000, // Default to 7 days ago
                endMs: currentTime,
            };
        }
        const sortedOperations = [...operations].sort((a, b) => a.lastUpdatedTimestamp - b.lastUpdatedTimestamp);
        return {
            startMs: sortedOperations[0].lastUpdatedTimestamp,
            endMs: sortedOperations[sortedOperations.length - 1].lastUpdatedTimestamp,
        };
    })();

    // Calculate bar data for individual operations
    const barData = useMemo(() => {
        return calculateBarData(operations, timeRange.startMs, timeRange.endMs);
    }, [operations, timeRange.startMs, timeRange.endMs]);

    const chartHeight = height - 100; // Subtract space for header/legend
    const chartInnerHeight = chartHeight - CHART_AXIS_BOTTOM_HEIGHT - CHART_AXIS_TOP_MARGIN;
    const chartInnerWidth = width - CHART_HORIZ_MARGIN - CHART_AXIS_LEFT_WIDTH;

    // Determine cadence and calculate Y scale
    const {
        maxY: cadenceMaxY,
        useDaysFormat,
        stepSizeMs,
    } = useMemo(() => {
        const cadence = determineCadence(barData);
        return {
            maxY: cadence.maxY,
            useDaysFormat: cadence.useDaysFormat,
            stepSizeMs: cadence.stepSizeMs,
        };
    }, [barData]);

    // X scale: evenly spaced events by index using band scale
    const xScale =
        barData.length === 0
            ? scaleBand<number>({
                  domain: [],
                  range: [0, chartInnerWidth],
              })
            : scaleBand<number>({
                  domain: barData.map((_, i) => i),
                  range: [0, chartInnerWidth],
                  padding: 0.1,
              });

    // Calculate Y scale based on cadence category
    // maxY is already rounded to step size in determineCadence
    const paddedMin = 0;
    const paddedMax = cadenceMaxY;

    const yScale = scaleLinear([paddedMin, paddedMax], [chartInnerHeight, 0]);

    // Generate whole number tick values using step size from cadence config
    const yAxisTickValues = useDaysFormat
        ? (() => {
              // Step size of 2 days for weekly cadence
              const maxDays = paddedMax / (24 * 60 * 60 * 1000);
              const stepDays = stepSizeMs / (24 * 60 * 60 * 1000);
              const ticks: number[] = [];
              for (let i = 0; i <= maxDays; i += stepDays) {
                  ticks.push(i * 24 * 60 * 60 * 1000);
              }
              return ticks;
          })()
        : (() => {
              // Step size of 4 hours for hourly and daily cadences
              const maxHours = paddedMax / (60 * 60 * 1000);
              const stepHours = stepSizeMs / (60 * 60 * 1000);
              const ticks: number[] = [];
              for (let i = 0; i <= maxHours; i += stepHours) {
                  ticks.push(i * 60 * 60 * 1000);
              }
              return ticks;
          })();

    // Generate date labels for x-axis (only first and last)
    const dateLabels = (() => {
        if (barData.length === 0) return [];
        if (barData.length === 1) return [0];
        return [0, barData.length - 1];
    })();

    // Drag selection state for zoom
    const [isSelecting, setIsSelecting] = useState(false);
    const [selectionStart, setSelectionStart] = useState<{ x: number; y: number } | null>(null);
    const [selectionEnd, setSelectionEnd] = useState<{ x: number; y: number } | null>(null);

    // Tooltip state
    const { tooltipData, tooltipLeft, tooltipTop, tooltipOpen, showTooltip, hideTooltip } = useTooltip<{
        timeSinceLastUpdate: number;
        updateTime: string;
    }>();

    const handleMouseDown = useCallback(
        (event: React.MouseEvent<HTMLDivElement>) => {
            if (!onRangeChange || !xScale) return;

            const rect = event.currentTarget.getBoundingClientRect();
            const x = event.clientX - rect.left;

            setIsSelecting(true);
            setSelectionStart({ x, y: 0 });
            setSelectionEnd({ x, y: 0 });
        },
        [onRangeChange, xScale],
    );

    const handleMouseMove = useCallback(
        (event: React.MouseEvent<HTMLDivElement>) => {
            if (!isSelecting || !selectionStart) return;

            const rect = event.currentTarget.getBoundingClientRect();
            const x = event.clientX - rect.left;
            const y = event.clientY - rect.top;

            setSelectionEnd({ x, y });
        },
        [isSelecting, selectionStart],
    );

    const handleMouseUp = useCallback(() => {
        if (!isSelecting || !selectionStart || !selectionEnd || !xScale || !onRangeChange || barData.length === 0)
            return;

        const minX = Math.min(selectionStart.x, selectionEnd.x);
        const maxX = Math.max(selectionStart.x, selectionEnd.x);

        // Only proceed if there's a meaningful selection (at least 10 pixels wide)
        if (maxX - minX > 10) {
            // Adjust x positions to account for chart margins
            const adjustedMinX = minX - CHART_HORIZ_MARGIN / 2 - CHART_AXIS_LEFT_WIDTH;
            const adjustedMaxX = maxX - CHART_HORIZ_MARGIN / 2 - CHART_AXIS_LEFT_WIDTH;

            // Find the barData entry closest to but >= the mouse start position
            let startData: (typeof barData)[0] | null = null;
            let minDistance = Infinity;
            for (let i = 0; i < barData.length; i++) {
                const barX = xScale(i);
                if (barX !== undefined) {
                    const barCenterX = barX + (xScale.bandwidth() || 0) / 2;
                    // Only consider bars that are at or to the right of the mouse start
                    if (barCenterX >= adjustedMinX) {
                        const distance = barCenterX - adjustedMinX;
                        if (distance < minDistance) {
                            minDistance = distance;
                            startData = barData[i];
                        }
                    }
                }
            }

            // Find the barData entry closest to but <= the mouse end position
            let endData: (typeof barData)[0] | null = null;
            minDistance = Infinity;
            for (let i = 0; i < barData.length; i++) {
                const barX = xScale(i);
                if (barX !== undefined) {
                    const barCenterX = barX + (xScale.bandwidth() || 0) / 2;
                    // Only consider bars that are at or to the left of the mouse end
                    if (barCenterX <= adjustedMaxX) {
                        const distance = adjustedMaxX - barCenterX;
                        if (distance < minDistance) {
                            minDistance = distance;
                            endData = barData[i];
                        }
                    }
                }
            }

            // Use found entries or fallback to first/last
            if (startData && endData && startData.timestampMillis <= endData.timestampMillis) {
                onRangeChange({
                    start: startData.timestampMillis,
                    end: endData.timestampMillis,
                });
            }
        }

        setIsSelecting(false);
        setSelectionStart(null);
        setSelectionEnd(null);
    }, [isSelecting, selectionStart, selectionEnd, xScale, onRangeChange, barData]);

    if (loading) {
        return (
            <div
                style={{
                    width,
                    height: height - 65,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                }}
            >
                <Loading marginTop={0} />
            </div>
        );
    }

    if (barData.length === 0) {
        return (
            <ChartContainer>
                <div
                    style={{
                        width,
                        height: height - 65,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                    }}
                >
                    <Text color="gray" colorLevel={600}>
                        No dataset update events found for the selected time range.
                    </Text>
                </div>
            </ChartContainer>
        );
    }

    return (
        <ChartContainer>
            <Legend>
                <LegendItem $color={BAR_COLOR}>Included in training set</LegendItem>
                <LegendItem $color={BAR_EXCEEDS_MAX_COLOR}>Included in training set, but later than usual</LegendItem>
                <LegendControlsContainer>
                    {resetRange && (
                        <ResetZoomButton onClick={resetRange} variant="secondary">
                            <ArrowCounterClockwise size={12} />
                            Reset Zoom
                        </ResetZoomButton>
                    )}
                    {resetRange === undefined && <ZoomInstructionText>Drag to zoom in</ZoomInstructionText>}
                </LegendControlsContainer>
            </Legend>
            <ChartInteractionContainer
                $isSelecting={isSelecting}
                onMouseDown={handleMouseDown}
                onMouseMove={handleMouseMove}
                onMouseUp={handleMouseUp}
                onMouseLeave={handleMouseUp}
                data-chart-container
            >
                {isSelecting && selectionStart && selectionEnd && (
                    <SelectionRectangle
                        $startX={selectionStart.x}
                        $endX={selectionEnd.x}
                        $height={chartHeight}
                        $marginTop={CHART_AXIS_TOP_MARGIN}
                        $marginBottom={CHART_AXIS_BOTTOM_HEIGHT}
                    />
                )}
                <svg width={width} height={chartHeight}>
                    <Group left={CHART_HORIZ_MARGIN / 2 + CHART_AXIS_LEFT_WIDTH} top={CHART_AXIS_TOP_MARGIN}>
                        {/* Y Axis */}
                        <AxisLeft
                            scale={yScale}
                            stroke={ANTD_GRAY[4]}
                            tickStroke={ANTD_GRAY[9]}
                            tickLength={4}
                            tickValues={yAxisTickValues}
                            tickFormat={(v) => formatTimeDuration(v.valueOf(), useDaysFormat)}
                            tickLabelProps={{
                                fill: ANTD_GRAY[9],
                                fontSize: 11,
                                textAnchor: 'end' as const,
                                dx: '-0.5em',
                            }}
                        />

                        {/* X Axis - show only first and last date labels */}
                        <AxisBottom
                            top={chartInnerHeight}
                            scale={xScale}
                            stroke={ANTD_GRAY[4]}
                            tickValues={dateLabels}
                            tickFormat={(index) => {
                                const indexValue = typeof index === 'number' ? index : (index as any).valueOf();
                                if (typeof indexValue !== 'number' || indexValue < 0 || indexValue >= barData.length) {
                                    return '';
                                }
                                const data = barData[indexValue];
                                return data ? moment(data.timestampMillis).format('MMM DD, YYYY') : '';
                            }}
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
                            tickValues={dateLabels}
                            height={chartInnerHeight}
                            lineStyle={{
                                stroke: ANTD_GRAY[5],
                                strokeLinecap: 'round',
                                strokeWidth: 1,
                                strokeDasharray: '1 4',
                            }}
                        />

                        {/* Clip path definition for bars that exceed max */}
                        <defs>
                            <clipPath id="chart-clip">
                                <rect x={0} y={0} width={chartInnerWidth} height={chartInnerHeight} />
                            </clipPath>
                        </defs>

                        {/* Bars */}
                        {barData.map((data, index) => {
                            const xOffset = xScale(index);
                            if (xOffset === undefined) return null;

                            // Check if this data point exceeds the max Y value
                            const exceedsMax = data.timeSinceLastUpdate > cadenceMaxY;

                            // Calculate bar height: timeSinceLastUpdate determines the height
                            // yScale maps timeSinceLastUpdate to y position, so we need to convert that to height
                            // If it exceeds max, cap it at the max for display
                            const cappedValue = exceedsMax ? cadenceMaxY : data.timeSinceLastUpdate;
                            const yPosition = yScale(cappedValue);
                            const barHeight = chartInnerHeight - yPosition;

                            // Calculate bar width and position
                            const barWidth = Math.max(12, Math.min(8, xScale.bandwidth() || 0));
                            const centerX = xOffset + (xScale.bandwidth() || 0) / 2;
                            const barX = centerX - barWidth / 2;

                            return (
                                <g
                                    key={data.timestampMillis}
                                    clipPath={exceedsMax ? 'url(#chart-clip)' : undefined}
                                    onMouseMove={(event) => {
                                        // Get the container element
                                        const container = event.currentTarget.closest(
                                            '[data-chart-container]',
                                        ) as HTMLElement;
                                        if (!container) return;

                                        const containerRect = container.getBoundingClientRect();

                                        // Use mouse coordinates relative to the container
                                        const tooltipX = event.clientX - containerRect.left;
                                        const tooltipY = event.clientY - containerRect.top - 50; // Position above the cursor

                                        showTooltip({
                                            tooltipData: {
                                                timeSinceLastUpdate: data.timeSinceLastUpdate,
                                                updateTime: data.updateTime,
                                            },
                                            tooltipLeft: tooltipX,
                                            tooltipTop: tooltipY,
                                        });
                                    }}
                                    onMouseLeave={hideTooltip}
                                >
                                    <Bar
                                        x={barX}
                                        y={yPosition}
                                        width={barWidth}
                                        height={barHeight}
                                        fill={exceedsMax ? BAR_EXCEEDS_MAX_COLOR : BAR_COLOR}
                                        stroke="white"
                                        strokeWidth={1}
                                    />
                                    {exceedsMax && (
                                        <>
                                            {/* From Chris Williams: https://github.com/airbnb/visx/discussions/1328#discussioncomment-1246375 */}
                                            <svg
                                                x={centerX - barWidth / 2}
                                                y={yPosition + CLIP_INDICATOR_HEIGHT}
                                                width={barWidth}
                                                height={CLIP_INDICATOR_HEIGHT}
                                                viewBox={`0 0 ${barWidth} ${CLIP_INDICATOR_HEIGHT}`}
                                                fill="none"
                                            >
                                                <path
                                                    d="M1 6L4.66667 2.25L8.33333 4.75L12 1"
                                                    stroke="white"
                                                    strokeWidth="1.5"
                                                />
                                            </svg>
                                        </>
                                    )}
                                </g>
                            );
                        })}
                    </Group>
                </svg>
                {tooltipOpen && tooltipData && (
                    <TooltipWithBounds
                        left={tooltipLeft}
                        top={tooltipTop}
                        style={{
                            backgroundColor: 'white',
                            padding: '8px 12px',
                            borderRadius: '4px',
                            boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
                            fontSize: '12px',
                            pointerEvents: 'none',
                            position: 'absolute',
                            zIndex: 10,
                        }}
                    >
                        <div>
                            <div style={{ marginBottom: '4px' }}>
                                <strong>Time since last update:</strong>{' '}
                                {formatTimeDuration(tooltipData.timeSinceLastUpdate)}
                            </div>
                            <div>
                                <strong>Update time:</strong> {tooltipData.updateTime}
                            </div>
                        </div>
                    </TooltipWithBounds>
                )}
            </ChartInteractionContainer>
        </ChartContainer>
    );
};
