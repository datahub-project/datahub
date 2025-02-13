import { colors } from '@src/alchemy-components/theme';
import { Axis } from '@visx/axis';
import { GridColumns } from '@visx/grid';
import { ParentSize } from '@visx/responsive';
import { scaleLinear } from '@visx/scale';
import { BoxPlot } from '@visx/stats';
import { useTooltip } from '@visx/tooltip';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import {
    AXIS_LABEL_MARGIN_OFFSET,
    AXIS_LABEL_PROPS,
    DEFAULT_BOX_SIZE,
    DEFAULT_GAP_BETWEEN_WHISKERS,
} from './constants';
import { whiskerChartDefaults } from './defaults';
import { InternalWhiskerChartProps, WhiskerChartProps, WhiskerTooltipDatum } from './types';
import { computeWhiskerOffset } from './utils';

const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
`;

function InternalWhiskerChart({
    data,
    width,
    height,
    tooltip,
    boxSize = whiskerChartDefaults.boxSize,
    gap = whiskerChartDefaults.gap,
    axisLabel,
    renderTooltip = whiskerChartDefaults.renderTooltip,
    renderWhisker = whiskerChartDefaults.renderWhisker,
}: InternalWhiskerChartProps) {
    const axisLabelMarginOffset = axisLabel !== undefined ? AXIS_LABEL_MARGIN_OFFSET : 0;
    const margin = { left: 10, top: 0, right: 10, bottom: 20 + axisLabelMarginOffset };

    const finalBoxSize = boxSize ?? DEFAULT_BOX_SIZE;
    const finalGap = gap ?? DEFAULT_GAP_BETWEEN_WHISKERS;

    const minY = 0;
    const maxY = height - margin.bottom;
    const minX = margin.left;
    const maxX = width - margin.right;
    const chartHeight = maxY - minY;
    const chartWidth = maxX - minX;

    const dataWithOffsets = useMemo(() => {
        return data.map((datum, index) => ({
            datum,
            offset: computeWhiskerOffset(data.length, index, finalBoxSize, chartHeight, finalGap),
        }));
    }, [data, chartHeight, finalBoxSize, finalGap]);

    const minValue = useMemo(() => Math.min(...data.map((datum) => datum.min)), [data]);
    const maxValue = useMemo(() => Math.max(...data.map((datum) => datum.max)), [data]);

    const xScale = useMemo(() => {
        // 5% paddings to left and right sides
        const valuePadding = (maxValue - minValue) * 0.05;
        return scaleLinear<number>({
            range: [minX, maxX],
            round: true,
            domain: [minValue - valuePadding, maxValue + valuePadding],
            nice: true,
        });
    }, [minX, maxX, minValue, maxValue]);

    return (
        <svg width={width} height={height}>
            <GridColumns
                scale={xScale}
                x={minX}
                y1={maxY}
                y2={minY}
                width={chartWidth}
                height={chartHeight}
                stroke={colors.gray[100]}
                numTicks={5}
            />

            {dataWithOffsets.map(({ datum, offset }) => (
                <BoxPlot
                    key={datum.key}
                    horizontal
                    boxWidth={finalBoxSize}
                    min={datum.min}
                    firstQuartile={datum.firstQuartile}
                    median={datum.median}
                    thirdQuartile={datum.thirdQuartile}
                    max={datum.max}
                    valueScale={xScale}
                    top={offset}
                >
                    {renderWhisker ? (props) => renderWhisker({ datum, tooltip, ...props }) : undefined}
                </BoxPlot>
            ))}

            <line x1={0} x2={width} y1={maxY} y2={maxY} strokeWidth={1} stroke={colors.gray[100]} />
            <Axis
                scale={xScale}
                top={maxY}
                hideTicks
                hideAxisLine
                orientation="bottom"
                numTicks={7}
                tickLabelProps={{
                    fontSize: '10px',
                    fontFamily: 'Mulish',
                    fill: colors.gray[1700],
                }}
                label={axisLabel}
                labelProps={AXIS_LABEL_PROPS}
            />

            {tooltip.tooltipOpen &&
                renderTooltip?.({
                    x: tooltip.tooltipLeft,
                    y: tooltip.tooltipTop,
                    minY,
                    maxY,
                    datum: tooltip.tooltipData,
                })}
        </svg>
    );
}

export default function WhiskerChart(props: WhiskerChartProps) {
    const tooltip = useTooltip<WhiskerTooltipDatum>();

    return (
        <ChartWrapper>
            <ParentSize>
                {({ width, height }) => (
                    <InternalWhiskerChart {...props} width={width} height={height} tooltip={tooltip} />
                )}
            </ParentSize>
        </ChartWrapper>
    );
}
