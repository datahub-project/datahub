import React, { useState } from 'react';
import { colors } from '@src/alchemy-components/theme';
import { TickLabelProps } from '@visx/axis';
import { LinearGradient } from '@visx/gradient';
import { ParentSize } from '@visx/responsive';
import { Axis, AxisScale, BarSeries, Grid, Tooltip, XYChart } from '@visx/xychart';
import dayjs from 'dayjs';
import { Popover } from '../Popover';
import { ChartWrapper, StyledBarSeries } from './components';
import { BarChartProps } from './types';
import { abbreviateNumber } from '../dataviz/utils';

const commonTickLabelProps: TickLabelProps<any> = {
    fontSize: 10,
    fontFamily: 'Mulish',
    fill: colors.gray[1700],
};

export const barChartDefault: BarChartProps<any> = {
    data: [],
    xAccessor: (datum) => datum?.x,
    yAccessor: (datum) => datum?.y,
    leftAxisTickFormat: abbreviateNumber,
    leftAxisTickLabelProps: {
        ...commonTickLabelProps,
        textAnchor: 'end',
    },
    bottomAxisTickFormat: (value) => dayjs(value).format('DD MMM'),
    bottomAxisTickLabelProps: {
        ...commonTickLabelProps,
        textAnchor: 'middle',
        verticalAnchor: 'start',
        width: 20,
    },
    barColor: 'url(#bar-gradient)',
    barSelectedColor: colors.violet[500],
    gridColor: '#e0e0e0',
    renderGradients: () => <LinearGradient id="bar-gradient" from={colors.violet[500]} to="#917FFF" toOpacity={0.6} />,
};

export function BarChart<DatumType extends object = any>({
    data,
    xAccessor = barChartDefault.xAccessor,
    yAccessor = barChartDefault.yAccessor,
    renderTooltipContent,
    margin,
    leftAxisTickFormat = barChartDefault.leftAxisTickFormat,
    leftAxisTickLabelProps = barChartDefault.leftAxisTickLabelProps,
    bottomAxisTickFormat = barChartDefault.bottomAxisTickFormat,
    bottomAxisTickLabelProps = barChartDefault.bottomAxisTickLabelProps,
    barColor = barChartDefault.barColor,
    barSelectedColor = barChartDefault.barSelectedColor,
    gridColor = barChartDefault.gridColor,
    renderGradients = barChartDefault.renderGradients,
}: BarChartProps<DatumType>) {
    const [hasSelectedBar, setHasSelectedBar] = useState<boolean>(false);

    // FYI: additional margins to show left and bottom axises
    const internalMargin = {
        top: (margin?.top ?? 0) + 30,
        right: margin?.right ?? 0,
        bottom: (margin?.bottom ?? 0) + 35,
        left: (margin?.left ?? 0) + 40,
    };

    const accessors = { xAccessor, yAccessor };

    return (
        <ChartWrapper>
            <ParentSize>
                {({ width, height }) => {
                    return (
                        <XYChart
                            width={width}
                            height={height}
                            xScale={{ type: 'band', paddingInner: 0.4, paddingOuter: 0.1 }}
                            yScale={{ type: 'linear', nice: true, round: true }}
                            margin={internalMargin}
                            captureEvents={false}
                        >
                            {renderGradients?.()}

                            <Axis
                                orientation="left"
                                hideAxisLine
                                hideTicks
                                tickFormat={leftAxisTickFormat}
                                tickLabelProps={leftAxisTickLabelProps}
                            />

                            <Axis
                                orientation="bottom"
                                numTicks={data.length}
                                tickFormat={bottomAxisTickFormat}
                                tickLabelProps={bottomAxisTickLabelProps}
                                hideAxisLine
                                hideTicks
                            />

                            <line
                                x1={internalMargin.left}
                                x2={internalMargin.left}
                                y1={0}
                                y2={height - internalMargin.bottom}
                                stroke={gridColor}
                            />

                            <Grid rows columns={false} stroke={gridColor} strokeWidth={1} lineStyle={{}} />

                            <StyledBarSeries
                                as={BarSeries<AxisScale, AxisScale, DatumType>}
                                $hasSelectedItem={hasSelectedBar}
                                $color={barColor}
                                $selectedColor={barSelectedColor}
                                dataKey="bar-seria-0"
                                data={data}
                                radius={4}
                                radiusTop
                                onBlur={() => setHasSelectedBar(false)}
                                onFocus={() => setHasSelectedBar(true)}
                                // Internally the library doesn't emmit these events if handlers are empty
                                // They are requred to show/hide/move tooltip
                                onPointerMove={() => null}
                                onPointerUp={() => null}
                                onPointerOut={() => null}
                                {...accessors}
                            />

                            <Tooltip<DatumType>
                                snapTooltipToDatumX
                                snapTooltipToDatumY
                                unstyled
                                applyPositionStyle
                                renderTooltip={({ tooltipData }) => {
                                    return (
                                        tooltipData?.nearestDatum && (
                                            <Popover
                                                open
                                                placement="topLeft"
                                                content={renderTooltipContent?.(tooltipData.nearestDatum.datum)}
                                            />
                                        )
                                    );
                                }}
                            />
                        </XYChart>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
}
