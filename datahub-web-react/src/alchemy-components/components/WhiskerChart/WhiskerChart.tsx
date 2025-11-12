import { Axis } from '@visx/axis';
import { GridColumns } from '@visx/grid';
import { Group } from '@visx/group';
import { ParentSize } from '@visx/responsive';
import { scaleLinear } from '@visx/scale';
import { BoxPlot } from '@visx/stats';
import { useTooltip } from '@visx/tooltip';
import { Margin } from '@visx/xychart';
import React, { useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import DynamicMarginSetter from '@components/components/BarChart/components/DynamicMarginSetter';
import {
    AXIS_LABEL_MARGIN_OFFSET,
    AXIS_LABEL_PROPS,
    DEFAULT_BOX_SIZE,
    DEFAULT_GAP_BETWEEN_WHISKERS,
} from '@components/components/WhiskerChart/constants';
import { whiskerChartDefaults } from '@components/components/WhiskerChart/defaults';
import {
    InternalWhiskerChartProps,
    WhiskerChartProps,
    WhiskerTooltipDatum,
} from '@components/components/WhiskerChart/types';
import { computeWhiskerOffset } from '@components/components/WhiskerChart/utils';
import { abbreviateNumber } from '@components/components/dataviz/utils';

import { colors } from '@src/alchemy-components/theme';

const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
`;

const NUMBER_OF_TICKS = 7;

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
    const wrapperRef = useRef<HTMLDivElement>(null);

    const defaultMargin = useMemo(() => {
        const axisLabelMarginOffset = axisLabel !== undefined ? AXIS_LABEL_MARGIN_OFFSET : 0;
        return {
            top: 0,
            right: 0,
            bottom: 20 + axisLabelMarginOffset,
            left: 0,
        };
    }, [axisLabel]);

    const [dynamicMargin, setDynamicMargin] = useState<Margin>(defaultMargin);

    const finalBoxSize = boxSize ?? DEFAULT_BOX_SIZE;
    const finalGap = gap ?? DEFAULT_GAP_BETWEEN_WHISKERS;

    const minY = 0;
    const maxY = height - dynamicMargin.bottom;
    const minX = dynamicMargin.left;
    const maxX = width - dynamicMargin.right;
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
        <div ref={wrapperRef}>
            <svg width={width} height={height}>
                <Group className="content-group">
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
                </Group>
                <Axis
                    scale={xScale}
                    top={maxY}
                    hideTicks
                    hideAxisLine
                    orientation="bottom"
                    numTicks={NUMBER_OF_TICKS}
                    tickFormat={abbreviateNumber}
                    tickLabelProps={{
                        fontSize: '10px',
                        fontFamily: 'Mulish',
                        fill: colors.gray[1700],
                    }}
                    label={axisLabel}
                    labelProps={AXIS_LABEL_PROPS}
                    tickClassName="bottom-axis-tick"
                />

                <DynamicMarginSetter
                    setMargin={setDynamicMargin}
                    wrapperRef={wrapperRef}
                    currentMargin={dynamicMargin}
                    minimalMargin={defaultMargin}
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
        </div>
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
