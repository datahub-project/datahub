import { BarChart as AlchemyBarChart, LineChart as AlchemyLineChart, GraphCard, Text } from '@components';
import { ParentSize } from '@visx/responsive';
import { useTooltip, useTooltipInPortal } from '@visx/tooltip';
import { Axis, BarSeries, BarStack, Grid, Tooltip, XYChart } from '@visx/xychart';
import dayjs from 'dayjs';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { Popover } from '@components/components/Popover';

import { TableChart } from '@app/analyticsDashboardV2/components/TableChart';
import { useAnalyticsChartColors } from '@app/analyticsDashboardV2/hooks/useAnalyticsChartColors';
import { colors } from '@src/alchemy-components/theme';

import { AnalyticsChart as AnalyticsChartType, BarChart as BarChartType, TimeSeriesChart } from '@types';

type Props = {
    chartData: AnalyticsChartType;
};

const TableWrapper = styled.div`
    height: 100%;
    overflow-y: auto;
    overflow-x: hidden;
`;

const ChartWithLegendContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    height: 100%;
    overflow: hidden;
`;

const ChartArea = styled.div`
    flex: 1;
    min-height: 0;
    width: 100%;
`;

const LegendArea = styled.div`
    width: 100%;
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    border-top: 1px solid ${colors.gray[200]};
    padding-top: 12px;
    margin-top: 8px;
    max-height: 80px;
`;

const LegendScrollArea = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    overflow-x: auto;
    overflow-y: hidden;
    padding: 4px 0;

    /* Custom scrollbar styling */
    &::-webkit-scrollbar {
        height: 6px;
    }

    &::-webkit-scrollbar-track {
        background: ${colors.gray[100]};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb {
        background: ${colors.gray[300]};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${colors.gray[400]};
    }

    /* Firefox scrollbar */
    scrollbar-width: thin;
    scrollbar-color: ${colors.gray[300]} ${colors.gray[100]};
`;

const LegendItem = styled.div<{ $isSelected?: boolean }>`
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 4px 8px;
    cursor: pointer;
    border-radius: 4px;
    transition: all 0.2s ease;
    white-space: nowrap;
    flex-shrink: 0;
    background-color: ${(props) => (props.$isSelected ? colors.violet[100] : 'transparent')};
    border: 1px solid ${(props) => (props.$isSelected ? colors.violet[400] : 'transparent')};

    &:hover {
        background-color: ${(props) => (props.$isSelected ? colors.violet[100] : colors.gray[50])};
    }
`;

const LegendColorBox = styled.div<{ color: string }>`
    width: 14px;
    height: 14px;
    background-color: ${(props) => props.color};
    border-radius: 3px;
    flex-shrink: 0;
    border: 1px solid rgba(0, 0, 0, 0.1);
`;

const LegendLabel = styled.span<{ $isSelected?: boolean }>`
    font-size: 13px;
    color: ${colors.gray[700]};
    line-height: 1.3;
    white-space: nowrap;
    font-weight: ${(props) => (props.$isSelected ? 'bold' : 'normal')};
`;

const TooltipContainer = styled.div`
    background: white;
    color: ${colors.gray[1700]};
    padding: 8px 12px;
    border-radius: 4px;
    font-size: 12px;
    pointer-events: none;
    z-index: 1000;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
    border: 1px solid ${colors.gray[300]};
`;

// Truncate label to max 11 characters total (based on "DATAPRODUCT" fitting perfectly)
// Since "..." takes 3 chars, we truncate to 8 chars + "..." = 11 total
const truncateLabel = (label: string, maxLength = 11): string => {
    if (label.length <= maxLength) return label;
    return `${label.substring(0, maxLength - 3)}...`;
};

type StackedBarChartProps = {
    stackedBarChartData: any[];
    allSegmentLabels: string[];
    segmentColors: string[];
};

const StackedBarChartWithTooltip = ({ stackedBarChartData, allSegmentLabels, segmentColors }: StackedBarChartProps) => {
    const { tooltipData, tooltipLeft, tooltipTop, showTooltip, hideTooltip } = useTooltip<{
        label: string;
        value: number;
    }>();
    const { containerRef, TooltipInPortal } = useTooltipInPortal({ scroll: true });
    const [selectedBar, setSelectedBar] = React.useState<number | null>(null);
    const [selectedSegments, setSelectedSegments] = React.useState<Set<string>>(new Set());

    // Create a state to store the container element for event listeners
    const [chartContainerElement, setChartContainerElement] = React.useState<HTMLDivElement | null>(null);

    // Filter data to show only the selected bar (for better detail visibility)
    const filteredBarChartData = useMemo(() => {
        if (selectedBar === null) return stackedBarChartData;
        return stackedBarChartData.filter((d) => d.x === selectedBar);
    }, [selectedBar, stackedBarChartData]);

    // Use effect to add event listeners after render
    React.useEffect(() => {
        let cleanupFn: (() => void) | null = null;

        // Need a small delay to ensure visx has rendered the DOM elements
        const timer = setTimeout(() => {
            if (!chartContainerElement) return;

            // visx renders each text element in its own SVG, so we need to find all text elements
            // that have the data-full-label attribute (which we set in tickLabelProps)
            const allTextElements = chartContainerElement.querySelectorAll('text[data-full-label]');

            if (!allTextElements || allTextElements.length === 0) {
                return;
            }

            const handleMouseEnterDOM = (event: Event) => {
                const target = event.currentTarget as SVGTextElement;
                const fullLabel = target.getAttribute('data-full-label');
                const displayedText = target.textContent || '';

                // Only show tooltip if label is truncated (ends with ...)
                if (fullLabel && displayedText.endsWith('...')) {
                    const rect = target.getBoundingClientRect();
                    const containerRect = chartContainerElement?.getBoundingClientRect();

                    if (containerRect) {
                        // Calculate position relative to the container
                        // Position tooltip well above the label for better visibility
                        showTooltip({
                            tooltipData: { label: fullLabel, value: 0 },
                            tooltipLeft: rect.left - containerRect.left + rect.width / 2,
                            tooltipTop: rect.top - containerRect.top - 50,
                        });
                    }
                }
            };

            const handleMouseLeaveDOM = () => {
                hideTooltip();
            };

            const handleClickDOM = () => {
                // Hide tooltip when clicking on the label
                hideTooltip();
            };

            allTextElements.forEach((label) => {
                label.addEventListener('mouseover', handleMouseEnterDOM);
                label.addEventListener('mouseout', handleMouseLeaveDOM);
                label.addEventListener('click', handleClickDOM);
            });

            // Store cleanup function
            cleanupFn = () => {
                allTextElements.forEach((label) => {
                    label.removeEventListener('mouseover', handleMouseEnterDOM);
                    label.removeEventListener('mouseout', handleMouseLeaveDOM);
                    label.removeEventListener('click', handleClickDOM);
                });
            };
        }, 100);

        return () => {
            clearTimeout(timer);
            if (cleanupFn) {
                cleanupFn();
            }
        };
    }, [chartContainerElement, showTooltip, hideTooltip, filteredBarChartData, selectedBar]);

    const handleBarClick = useCallback((barIndex: number) => {
        setSelectedBar((prev) => (prev === barIndex ? null : barIndex));
    }, []);

    const handleLegendClick = useCallback((segmentLabel: string) => {
        setSelectedSegments((prev) => {
            const newSet = new Set(prev);
            if (newSet.has(segmentLabel)) {
                newSet.delete(segmentLabel);
            } else {
                newSet.add(segmentLabel);
            }
            return newSet;
        });
    }, []);

    // Filter legend to show only tabs with data for the selected bar
    const filteredSegmentLabels = useMemo(() => {
        if (selectedBar === null) return allSegmentLabels;

        const selectedBarData = stackedBarChartData[selectedBar];
        if (!selectedBarData) return allSegmentLabels;

        return allSegmentLabels.filter((label) => (selectedBarData[label] || 0) > 0);
    }, [selectedBar, stackedBarChartData, allSegmentLabels]);

    // Determine which segments to render in the chart
    const visibleSegmentLabels = useMemo(() => {
        if (selectedSegments.size === 0) return allSegmentLabels;
        return allSegmentLabels.filter((label) => selectedSegments.has(label));
    }, [selectedSegments, allSegmentLabels]);

    // Callback ref to set both refs
    const setContainerRefs = useCallback(
        (node: HTMLDivElement | null) => {
            setChartContainerElement(node);
            // Call the containerRef from useTooltipInPortal
            if (typeof containerRef === 'function') {
                containerRef(node);
            }
        },
        [containerRef],
    );

    return (
        <ChartWithLegendContainer>
            <ChartArea ref={setContainerRefs}>
                <ParentSize>
                    {({ width, height }) =>
                        width > 0 && height > 0 ? (
                            <>
                                <XYChart
                                    width={width}
                                    height={height}
                                    xScale={{ type: 'band', paddingInner: 0.3 }}
                                    yScale={{ type: 'linear' }}
                                    margin={{ top: 20, right: 20, bottom: 80, left: 60 }}
                                >
                                    <Grid columns={false} numTicks={5} lineStyle={{ stroke: '#EAEAEA' }} />
                                    <Axis
                                        orientation="bottom"
                                        tickFormat={(tickValue) => {
                                            const bar = filteredBarChartData.find((d) => d.x === tickValue);
                                            if (!bar) return String(tickValue);
                                            // If a bar is selected, show full label; otherwise truncate
                                            return selectedBar !== null ? bar.label : truncateLabel(bar.label);
                                        }}
                                        tickLabelProps={(tickValue) => {
                                            const bar = filteredBarChartData.find((d) => d.x === tickValue);
                                            const fullLabel = bar?.label || String(tickValue);
                                            const isSelected = selectedBar === tickValue;
                                            // When a bar is selected, make label horizontal; otherwise angled
                                            return {
                                                angle: selectedBar !== null ? 0 : -45,
                                                textAnchor: selectedBar !== null ? 'middle' : 'end',
                                                fontSize: 11,
                                                fontWeight: isSelected ? 'bold' : 'normal',
                                                textDecoration: isSelected ? 'underline' : 'none',
                                                fill: isSelected ? colors.violet[600] : colors.gray[700],
                                                onClick: () => handleBarClick(tickValue as number),
                                                style: { cursor: 'pointer' },
                                                pointerEvents: 'all',
                                                'data-full-label': fullLabel,
                                            };
                                        }}
                                    />
                                    <Axis orientation="left" numTicks={5} />
                                    <BarStack>
                                        {visibleSegmentLabels.map((segmentLabel) => {
                                            const idx = allSegmentLabels.indexOf(segmentLabel);
                                            return (
                                                <BarSeries
                                                    key={segmentLabel}
                                                    dataKey={segmentLabel}
                                                    data={filteredBarChartData}
                                                    xAccessor={(d) => d.x}
                                                    yAccessor={(d) => d[segmentLabel] || 0}
                                                    colorAccessor={() => segmentColors[idx % segmentColors.length]}
                                                />
                                            );
                                        })}
                                    </BarStack>
                                    <Tooltip
                                        key={Math.random()}
                                        snapTooltipToDatumX
                                        snapTooltipToDatumY
                                        unstyled
                                        applyPositionStyle
                                        renderTooltip={({ tooltipData: visxTooltipData }) => {
                                            if (!visxTooltipData?.nearestDatum) return null;
                                            const { datum } = visxTooltipData.nearestDatum;
                                            const segmentKey = visxTooltipData.nearestDatum.key;
                                            const value = (datum as any)[segmentKey] || 0;

                                            if (value === 0) return null;

                                            return (
                                                <Popover
                                                    open
                                                    defaultOpen
                                                    placement="topLeft"
                                                    key={`${(datum as any).x}-${segmentKey}`}
                                                    content={
                                                        <>
                                                            <Text weight="bold">{segmentKey}</Text>
                                                            <Text>{value.toLocaleString()}</Text>
                                                        </>
                                                    }
                                                />
                                            );
                                        }}
                                    />
                                </XYChart>
                                {tooltipData && (
                                    <TooltipInPortal key={Math.random()} top={tooltipTop} left={tooltipLeft}>
                                        <TooltipContainer>
                                            {tooltipData.value > 0 ? (
                                                <>
                                                    <Text weight="bold">{tooltipData.label}</Text>
                                                    <Text>{tooltipData.value.toLocaleString()}</Text>
                                                </>
                                            ) : (
                                                <Text>{tooltipData.label}</Text>
                                            )}
                                        </TooltipContainer>
                                    </TooltipInPortal>
                                )}
                            </>
                        ) : null
                    }
                </ParentSize>
            </ChartArea>
            <LegendArea>
                <LegendScrollArea>
                    {filteredSegmentLabels.map((segmentLabel) => {
                        const idx = allSegmentLabels.indexOf(segmentLabel);
                        const isSelected = selectedSegments.has(segmentLabel);
                        return (
                            <LegendItem
                                key={segmentLabel}
                                $isSelected={isSelected}
                                onClick={() => handleLegendClick(segmentLabel)}
                            >
                                <LegendColorBox color={segmentColors[idx % segmentColors.length]} />
                                <LegendLabel $isSelected={isSelected}>{segmentLabel}</LegendLabel>
                            </LegendItem>
                        );
                    })}
                </LegendScrollArea>
            </LegendArea>
        </ChartWithLegendContainer>
    );
};

export const AnalyticsChart = ({ chartData }: Props) => {
    const isTable = chartData.__typename === 'TableChart';

    const isEmpty = useMemo(() => {
        if (chartData.__typename === 'TimeSeriesChart') {
            return chartData.lines.every((line) => line.data.length === 0);
        }
        if (chartData.__typename === 'BarChart') {
            return chartData.bars.length === 0;
        }
        return false;
    }, [chartData]);

    const timeSeriesData = useMemo(() => {
        if (chartData.__typename !== 'TimeSeriesChart') return [];
        const firstLine = (chartData as TimeSeriesChart).lines[0];
        if (!firstLine) return [];
        return firstLine.data.map((point) => ({
            x: new Date(point.x).getTime(),
            y: point.y,
        }));
    }, [chartData]);

    const barChartData = useMemo(() => {
        if (chartData.__typename !== 'BarChart') return [];
        return (chartData as BarChartType).bars.map((bar, index) => ({
            x: index,
            y: bar.segments.reduce((sum, segment) => sum + segment.value, 0),
            label: bar.name,
        }));
    }, [chartData]);

    const isStackedBarChart = useMemo(() => {
        if (chartData.__typename !== 'BarChart') return false;
        const { bars } = chartData as BarChartType;
        // A chart should be stacked if any bar has multiple segments
        return bars.some((bar) => bar.segments.length > 1);
    }, [chartData]);

    const stackedBarChartData = useMemo(() => {
        if (chartData.__typename !== 'BarChart' || !isStackedBarChart) return [];
        const { bars } = chartData as BarChartType;

        // Transform data for stacked bar chart
        // Each segment becomes a separate series
        const allSegmentLabels = Array.from(new Set(bars.flatMap((bar) => bar.segments.map((seg) => seg.label))));

        return bars.map((bar, index) => {
            const dataPoint: any = {
                x: index,
                label: bar.name,
            };

            // Add each segment as a property
            allSegmentLabels.forEach((segLabel) => {
                const segment = bar.segments.find((s) => s.label === segLabel);
                dataPoint[segLabel] = segment ? segment.value : 0;
            });

            return dataPoint;
        });
    }, [chartData, isStackedBarChart]);

    if (chartData.__typename === 'TimeSeriesChart') {
        return (
            <GraphCard
                title={chartData.title}
                isEmpty={isEmpty}
                graphHeight="350px"
                renderGraph={() => (
                    <AlchemyLineChart
                        data={timeSeriesData}
                        bottomAxisProps={{
                            tickFormat: (x) => dayjs(x).format('MMM D'),
                        }}
                        popoverRenderer={(datum) => (
                            <>
                                <Text weight="bold">{dayjs(datum.x).format('MMM D, YYYY')}</Text>
                                <Text>{datum.y.toLocaleString()}</Text>
                            </>
                        )}
                    />
                )}
            />
        );
    }

    if (chartData.__typename === 'BarChart') {
        if (isStackedBarChart) {
            const { bars } = chartData as BarChartType;
            const allSegmentLabels = Array.from(new Set(bars.flatMap((bar) => bar.segments.map((seg) => seg.label))));

            // Use smart color assignment for stacked bar chart segments
            // eslint-disable-next-line react-hooks/rules-of-hooks
            const { getColorByKey } = useAnalyticsChartColors(allSegmentLabels);
            const segmentColors = allSegmentLabels.map((label) => getColorByKey(label));

            return (
                <GraphCard
                    title={chartData.title}
                    isEmpty={isEmpty}
                    graphHeight="350px"
                    renderGraph={() => (
                        <StackedBarChartWithTooltip
                            stackedBarChartData={stackedBarChartData}
                            allSegmentLabels={allSegmentLabels}
                            segmentColors={segmentColors}
                        />
                    )}
                />
            );
        }

        return (
            <GraphCard
                title={chartData.title}
                isEmpty={isEmpty}
                graphHeight="350px"
                renderGraph={() => (
                    <AlchemyBarChart
                        data={barChartData}
                        margin={{ bottom: 80 }}
                        bottomAxisProps={{
                            tickFormat: (x) => {
                                const bar = barChartData.find((d) => d.x === x);
                                const label = bar?.label || String(x);
                                return truncateLabel(label);
                            },
                            tickLabelProps: {
                                angle: -45,
                                textAnchor: 'end',
                                fontSize: 11,
                                fill: colors.gray[700],
                            },
                        }}
                        popoverRenderer={(datum) => {
                            const bar = barChartData.find((d) => d.x === datum.x);
                            return (
                                <>
                                    <Text weight="bold">{bar?.label || String(datum.x)}</Text>
                                    <Text>{datum.y.toLocaleString()}</Text>
                                </>
                            );
                        }}
                    />
                )}
            />
        );
    }

    if (isTable) {
        return (
            <GraphCard
                title={chartData.title}
                isEmpty={false}
                graphHeight="350px"
                renderGraph={() => (
                    <TableWrapper>
                        <TableChart chartData={chartData} />
                    </TableWrapper>
                )}
            />
        );
    }

    return null;
};
