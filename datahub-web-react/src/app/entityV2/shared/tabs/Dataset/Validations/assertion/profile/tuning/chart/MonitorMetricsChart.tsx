import { colors } from '@components';
import { ExclamationMark, ExcludeSquare, UniteSquare } from '@phosphor-icons/react';
import { Axis, GlyphSeries, LineSeries, Tooltip, XYChart } from '@visx/xychart';
import moment from 'moment';
import { MagnifyingGlass } from 'phosphor-react';
import React, { useCallback, useMemo, useState } from 'react';
import styled, { css } from 'styled-components';

import { AssertionPrediction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/useInferenceRegenerationPoller';
import { FuturePredictionTooltip } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/FuturePredictionTooltip';
import { MetricDataTooltip } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/MetricDataTooltip';
import { MonitorMetricsChartHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/MonitorMetricsChartHeader';
import { PredictionOverlays } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/PredictionOverlays';
import {
    DEFAULT_AXIS_COLOR,
    MARGIN,
    METRICS_LINE_COLOR,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/constants';
import {
    MetricDataPoint,
    calculateVisibleExclusionWindows,
    calculateXAxisDomain,
    calculateYAxisDomain,
    createTimeScale,
    formatAxisDate,
    formatMetricNumber,
    getFillColor,
    getFuturePredictions,
    isTimestampInExclusionWindow,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/utils';
import Loading from '@app/shared/Loading';

import { AssertionExclusionWindow } from '@types';

const ChartContainer = styled.div`
    width: 100%;
`;

const ChartInteractionContainer = styled.div<{ hasZoom: boolean }>`
    position: relative;
    ${(props) =>
        props.hasZoom &&
        css`
            cursor: crosshair;
        `}
`;

const NoDataOverlay = styled.div<{ hasDateRange: boolean }>`
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #8c8c8c;
    font-size: 14px;
    pointer-events: none;

    &::after {
        content: '${(props) =>
            props.hasDateRange
                ? 'No metrics data available for the selected date range'
                : 'No metrics data available'}';
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
    border: 1px solid ${METRICS_LINE_COLOR};
    pointer-events: none;
`;

const ActionMenuBackdrop = styled.div`
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 1000;
`;

const ActionMenu = styled.div<{ x: number; y: number }>`
    position: absolute;
    left: ${(props) => props.x}px;
    top: ${(props) => props.y}px;
    background: white;
    border-radius: 6px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    z-index: 1001;
    font-size: 12px;
    overflow: hidden;
    transform: translateX(-50%); // Center horizontally over selection
`;

const ActionButton = styled.button`
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    padding: 8px 12px;
    border: none;
    background: white;
    color: ${colors.gray[700]};
    cursor: pointer;
    font-size: 14px;
    font-weight: 600;
    text-align: left;
    transition: background-color 0.2s ease;

    &:hover {
        background-color: ${colors.primary[0]};
    }

    &:first-child {
        border-top-left-radius: 5px;
        border-top-right-radius: 5px;
    }

    &:last-child {
        border-bottom-left-radius: 5px;
        border-bottom-right-radius: 5px;
    }
`;

const LoadingOverlay = styled.div`
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background-color: rgba(255, 255, 255, 0.8);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
`;

const ExclusionWindowOverlay = styled.div<{
    leftPercent: number;
    widthPercent: number;
    width: number;
    height: number;
}>`
    position: absolute;
    left: ${(props) => MARGIN.left + (props.leftPercent / 100) * (props.width - MARGIN.left - MARGIN.right)}px;
    top: ${MARGIN.top}px;
    width: ${(props) => (props.widthPercent / 100) * (props.width - MARGIN.left - MARGIN.right)}px;
    height: ${(props) => props.height - MARGIN.top - MARGIN.bottom}px;
    background-color: ${colors.gray[300]};
    opacity: 0.3;
    z-index: 1;
    pointer-events: none;
`;

type Props = {
    metrics: MetricDataPoint[];
    predictions: AssertionPrediction[];
    exclusionWindows: AssertionExclusionWindow[];
    loading?: boolean;
    height?: number;
    width?: number;
    onRangeChange?: (range: { start: number; end: number }) => void;
    resetRange?: () => void;
    onDateRangeChange?: (startDate: moment.Moment | null, endDate: moment.Moment | null) => void;
    onAddExclusionWindow?: (startTimeMillis: number, endTimeMillis: number) => void;
    onBulkUnmarkAnomalies?: (startTimeMillis: number, endTimeMillis: number) => void;
    dateRange?: [moment.Moment | null, moment.Moment | null];
    currentTime?: number;
    latestMetricTimestamp?: number | null;
};

export const MonitorMetricsChart: React.FC<Props> = ({
    metrics,
    predictions,
    exclusionWindows,
    loading = false,
    height = 400,
    width = 700,
    onRangeChange,
    resetRange,
    onDateRangeChange,
    onAddExclusionWindow,
    onBulkUnmarkAnomalies,
    dateRange,
    currentTime,
    latestMetricTimestamp,
}) => {
    // Filter out undefined or invalid metric data points
    const validMetrics = useMemo(() => {
        return metrics.filter((m) => m && typeof m.x === 'string' && typeof m.y === 'number');
    }, [metrics]);

    const hasAnomalies = useMemo(() => validMetrics.some((m) => m.hasAnomaly), [validMetrics]);
    const [isSelecting, setIsSelecting] = useState(false);
    const [selectionStart, setSelectionStart] = useState<{ x: number; y: number } | null>(null);
    const [selectionEnd, setSelectionEnd] = useState<{ x: number; y: number } | null>(null);

    // Action menu state for drag selection
    const [actionMenu, setActionMenu] = useState<{
        show: boolean;
        x: number;
        y: number;
        startTime: number;
        endTime: number;
    } | null>(null);

    // Prediction tooltip state
    const [hoveredPrediction, setHoveredPrediction] = useState<{
        prediction: AssertionPrediction;
        mousePosition: { x: number; y: number };
    } | null>(null);

    // Date range picker state - sync with external dateRange prop
    const [internalDateRange, setInternalDateRange] = useState<[moment.Moment | null, moment.Moment | null]>(
        dateRange || [null, null],
    );

    // Sync internal date range with external prop when it changes
    React.useEffect(() => {
        if (dateRange) {
            setInternalDateRange(dateRange);
        }
    }, [dateRange]);

    // Check if we should show future predictions
    const shouldShowPredictions = useMemo(() => {
        if (!currentTime || !latestMetricTimestamp) return true; // Default to showing predictions if no timestamps provided

        const endTime = internalDateRange[1]?.valueOf() || new Date().getTime();

        // Always show predictions if range ends in the future
        if (endTime > currentTime) return true;

        // Show predictions if range ends after the latest available metric data
        return endTime > latestMetricTimestamp;
    }, [internalDateRange, currentTime, latestMetricTimestamp]);

    // Filter predictions to only include future ones within the date range, and only if we should show them
    const futurePredictions = useMemo(() => {
        if (!shouldShowPredictions) return [];
        return getFuturePredictions(predictions, internalDateRange);
    }, [predictions, shouldShowPredictions, internalDateRange]);

    // Create x-axis domain based on selected date range or data range
    const xAxisDomain = useMemo(() => {
        return calculateXAxisDomain(internalDateRange, validMetrics, futurePredictions);
    }, [internalDateRange, validMetrics, futurePredictions]);

    // Create y-axis domain with buffer based on metrics and predictions
    const yAxisDomain = useMemo(() => {
        return calculateYAxisDomain(validMetrics, futurePredictions, 0.15); // 15% buffer
    }, [validMetrics, futurePredictions]);

    // Calculate visible exclusion windows within the current x-axis domain
    const visibleExclusionWindows = useMemo(() => {
        return calculateVisibleExclusionWindows(exclusionWindows, xAxisDomain);
    }, [exclusionWindows, xAxisDomain]);

    // Create scales for zoom functionality
    const timeScale = useMemo(() => {
        return createTimeScale(xAxisDomain, width, MARGIN);
    }, [xAxisDomain, width]);

    const handleMouseDown = useCallback(
        (event: React.MouseEvent<HTMLDivElement>) => {
            if (!onRangeChange || !timeScale) return;

            const rect = event.currentTarget.getBoundingClientRect();
            const x = event.clientX - rect.left;
            const y = event.clientY - rect.top;

            setIsSelecting(true);
            setSelectionStart({ x, y });
            setSelectionEnd({ x, y });
        },
        [onRangeChange, timeScale],
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

    const handleMouseUp = useCallback(
        (_) => {
            if (!isSelecting || !selectionStart || !selectionEnd || !timeScale || !onRangeChange) return;

            const minX = Math.min(selectionStart.x, selectionEnd.x);
            const maxX = Math.max(selectionStart.x, selectionEnd.x);

            // Only proceed if there's a meaningful selection (at least 10 pixels wide)
            if (maxX - minX > 10) {
                const startTime = timeScale.invert(minX);
                const endTime = timeScale.invert(maxX);

                // Show action menu instead of immediately zooming
                const centerX = (minX + maxX) / 2;
                const menuY = selectionStart.y;

                setActionMenu({
                    show: true,
                    x: centerX,
                    y: menuY,
                    startTime: startTime.getTime(),
                    endTime: endTime.getTime(),
                });
            } else {
                setSelectionStart(null);
                setSelectionEnd(null);
            }

            setIsSelecting(false);
        },
        [isSelecting, selectionStart, selectionEnd, timeScale, onRangeChange],
    );

    const handleResetZoom = useCallback(() => {
        if (resetRange) {
            resetRange();
        }
    }, [resetRange]);

    // Action menu handlers
    const handleZoomAction = useCallback(() => {
        if (actionMenu && onRangeChange) {
            onRangeChange({
                start: actionMenu.startTime,
                end: actionMenu.endTime,
            });
        }
        setActionMenu(null);
        setSelectionStart(null);
        setSelectionEnd(null);
    }, [actionMenu, onRangeChange]);

    const handleExcludeAction = useCallback(() => {
        if (onAddExclusionWindow && actionMenu) {
            onAddExclusionWindow(actionMenu.startTime, actionMenu.endTime);
        }
        setActionMenu(null);
        setSelectionStart(null);
        setSelectionEnd(null);
    }, [actionMenu, onAddExclusionWindow]);

    const handleIncludeAction = useCallback(() => {
        if (onBulkUnmarkAnomalies && actionMenu) {
            onBulkUnmarkAnomalies(actionMenu.startTime, actionMenu.endTime);
        }
        setActionMenu(null);
        setSelectionStart(null);
        setSelectionEnd(null);
    }, [actionMenu, onBulkUnmarkAnomalies]);

    // Close action menu when clicking outside
    const handleCloseActionMenu = useCallback(() => {
        setActionMenu(null);
        setSelectionStart(null);
        setSelectionEnd(null);
    }, []);

    // Date range picker handlers
    const handleDateRangeChange = useCallback(
        (dates: [moment.Moment | null, moment.Moment | null] | null) => {
            const [startDate, endDate] = dates || [null, null];
            setInternalDateRange([startDate, endDate]);

            if (onDateRangeChange) {
                onDateRangeChange(startDate, endDate);
            }
        },
        [onDateRangeChange],
    );

    return (
        <ChartContainer>
            <MonitorMetricsChartHeader
                hasAnomalies={hasAnomalies}
                futurePredictionsCount={futurePredictions.length}
                resetRange={resetRange}
                onRangeChange={onRangeChange}
                onResetZoom={handleResetZoom}
                dateRange={internalDateRange}
                onDateRangeChange={handleDateRangeChange}
            />
            <ChartInteractionContainer
                hasZoom={!!onRangeChange}
                onMouseDown={onRangeChange ? handleMouseDown : undefined}
                onMouseMove={onRangeChange ? handleMouseMove : undefined}
                onMouseUp={onRangeChange ? handleMouseUp : undefined}
            >
                <XYChart
                    width={width}
                    height={height}
                    margin={MARGIN}
                    xScale={{ type: 'time', domain: xAxisDomain }}
                    yScale={{ type: 'linear', domain: yAxisDomain, zero: false }}
                >
                    <Axis
                        orientation="bottom"
                        stroke={DEFAULT_AXIS_COLOR}
                        strokeWidth={1}
                        tickLabelProps={{
                            fill: colors.gray[1700],
                            fontFamily: 'inherit',
                            fontSize: 12,
                            textAnchor: 'middle',
                        }}
                        numTicks={4}
                        tickFormat={formatAxisDate}
                    />
                    <Axis
                        orientation="left"
                        stroke={DEFAULT_AXIS_COLOR}
                        strokeWidth={1}
                        tickFormat={formatMetricNumber}
                        tickLabelProps={{
                            fill: colors.gray[1700],
                            fontFamily: 'inherit',
                            fontSize: 12,
                            textAnchor: 'end',
                        }}
                        numTicks={4}
                    />

                    {/* Metric values line */}
                    <LineSeries
                        dataKey="metrics"
                        data={validMetrics}
                        xAccessor={(d) => (d ? new Date(d.x) : new Date())}
                        yAccessor={(d) => (d ? d.y : 0)}
                        stroke={METRICS_LINE_COLOR}
                        strokeWidth={2}
                    />

                    {/* Metric data points with anomaly indicators */}
                    <GlyphSeries
                        dataKey="metrics"
                        data={validMetrics}
                        xAccessor={(d) => (d ? new Date(d.x) : new Date())}
                        yAccessor={(d) => (d ? d.y : 0)}
                        size={12}
                        renderGlyph={({ x, y, datum }) => {
                            const point = datum as MetricDataPoint;
                            const isInExclusionWindow = isTimestampInExclusionWindow(
                                new Date(point.x).getTime(),
                                exclusionWindows,
                            );
                            const fillColor = getFillColor(point, exclusionWindows);

                            return (
                                <g>
                                    <circle
                                        cx={x}
                                        cy={y}
                                        r={6}
                                        fill={fillColor}
                                        stroke={colors.violet[100]}
                                        strokeWidth={1}
                                    />
                                    {point.hasAnomaly && !isInExclusionWindow && (
                                        <foreignObject x={x - 5} y={y - 9} width={12} height={12}>
                                            <ExclamationMark size={9} weight="bold" color="white" />
                                        </foreignObject>
                                    )}
                                </g>
                            );
                        }}
                    />

                    <Tooltip
                        // Force tooltip bounds to update when domain changes
                        key={`${xAxisDomain[0].getTime()}-${xAxisDomain[1].getTime()}`}
                        snapTooltipToDatumX
                        snapTooltipToDatumY
                        showVerticalCrosshair
                        showSeriesGlyphs
                        glyphStyle={{
                            fill: METRICS_LINE_COLOR,
                            stroke: 'white',
                            strokeWidth: 1,
                        }}
                        renderGlyph={({ x, y, datum }) => {
                            const point = datum as MetricDataPoint;
                            const fillColor = getFillColor(point, exclusionWindows);
                            return <circle cx={x} cy={y} r={6} fill={fillColor} stroke="white" strokeWidth={2} />;
                        }}
                        renderTooltip={({ tooltipData }) => {
                            const nearestDatum = tooltipData?.nearestDatum;
                            if (!nearestDatum) return null;

                            const datum = nearestDatum.datum as MetricDataPoint;
                            return <MetricDataTooltip datum={datum} exclusionWindows={exclusionWindows} />;
                        }}
                    />
                </XYChart>

                {/* Exclusion window overlays */}
                {visibleExclusionWindows.map((exclusionWindow) => (
                    <ExclusionWindowOverlay
                        key={`exclusion-window-${exclusionWindow.startTime}-${exclusionWindow.endTime}`}
                        leftPercent={exclusionWindow.leftPercent}
                        widthPercent={exclusionWindow.widthPercent}
                        width={width}
                        height={height}
                    />
                ))}

                {/* Future predictions overlay */}
                <PredictionOverlays
                    futurePredictions={futurePredictions}
                    xAxisDomain={xAxisDomain}
                    yAxisDomain={yAxisDomain}
                    width={width}
                    height={height}
                    hoveredPrediction={hoveredPrediction}
                    onPredictionHover={(prediction, mousePosition) => {
                        setHoveredPrediction({
                            prediction,
                            mousePosition,
                        });
                    }}
                    onPredictionLeave={() => {
                        setHoveredPrediction(null);
                    }}
                />

                {/* No data overlay */}
                {validMetrics.length === 0 && (
                    <NoDataOverlay hasDateRange={!!(internalDateRange[0] && internalDateRange[1])} />
                )}

                {/* Selection rectangle during drag - rendered on top */}
                {selectionStart && selectionEnd && (
                    <SelectionRectangle
                        startX={selectionStart.x}
                        endX={selectionEnd.x}
                        height={height}
                        marginTop={MARGIN.top}
                        marginBottom={MARGIN.bottom}
                    />
                )}

                {/* Prediction tooltip */}
                {hoveredPrediction && (
                    <FuturePredictionTooltip
                        prediction={hoveredPrediction.prediction}
                        mousePosition={hoveredPrediction.mousePosition}
                    />
                )}

                {/* Action menu */}
                {actionMenu?.show && (
                    <>
                        {/* Backdrop to close menu when clicking outside */}
                        <ActionMenuBackdrop onClick={handleCloseActionMenu} />
                        <ActionMenu x={actionMenu.x} y={actionMenu.y}>
                            <ActionButton onClick={handleZoomAction}>
                                <MagnifyingGlass size={16} weight="bold" />
                                Zoom
                            </ActionButton>
                            <ActionButton onClick={handleExcludeAction}>
                                <ExcludeSquare size={16} weight="fill" />
                                Exclude Metrics
                            </ActionButton>
                            <ActionButton onClick={handleIncludeAction}>
                                <UniteSquare size={16} weight="fill" />
                                Include Metrics
                            </ActionButton>
                        </ActionMenu>
                    </>
                )}

                {/* Loading overlay */}
                {loading && (
                    <LoadingOverlay>
                        <Loading height={24} marginTop={0} />
                    </LoadingOverlay>
                )}
            </ChartInteractionContainer>
        </ChartContainer>
    );
};
