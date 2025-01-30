import React, { useState } from 'react';
import { colors } from '@src/alchemy-components/theme';
import { abbreviateNumber } from '@src/app/dataviz/utils';
import { TickLabelProps } from '@visx/axis';
import { LinearGradient } from '@visx/gradient';
import { ParentSize } from '@visx/responsive';
import { Axis, AxisScale, BarSeries, Grid, Tooltip, XYChart } from '@visx/xychart';
import dayjs from 'dayjs';
import { Popover } from '../Popover';
import { ChartWrapper, StyledBarSeries } from './components';
import { AxisProps, BarChartProps } from './types';
import { getMockedProps } from './utils';
import useMergedProps from './hooks/useMergedProps';
import useAdaptYScaleToZeroValues from './hooks/useAdaptYScaleToZeroValues';
import useAdaptYAccessorToZeroValue from './hooks/useAdaptYAccessorToZeroValues';
import useMaxDataValue from './hooks/useMaxDataValue';

const commonTickLabelProps: TickLabelProps<any> = {
    fontSize: 10,
    fontFamily: 'Mulish',
    fill: colors.gray[1700],
};

export const barChartDefault: BarChartProps<any> = {
    data: [],

    xAccessor: (datum) => datum?.x,
    yAccessor: (datum) => datum?.y,
    xScale: { type: 'band', paddingInner: 0.4, paddingOuter: 0.1 },
    yScale: { type: 'linear', nice: true, round: true },

    barColor: 'url(#bar-gradient)',
    barSelectedColor: colors.violet[500],

    leftAxisProps: {
        tickFormat: abbreviateNumber,
        tickLabelProps: {
            ...commonTickLabelProps,
            textAnchor: 'end',
        },
        hideAxisLine: true,
        hideTicks: true,
    },
    bottomAxisProps: {
        tickFormat: (value) => dayjs(value).format('DD MMM'),
        tickLabelProps: {
            ...commonTickLabelProps,
            textAnchor: 'middle',
            verticalAnchor: 'start',
            width: 20,
        },
        hideAxisLine: true,
        hideTicks: true,
    },
    gridProps: {
        rows: true,
        columns: false,
        stroke: '#e0e0e0',
        strokeWidth: 1,
        lineStyle: {},
    },

    renderGradients: () => <LinearGradient id="bar-gradient" from={colors.violet[500]} to="#917FFF" toOpacity={0.6} />,
};

export function BarChart<DatumType extends object = any>({
    data,
    isEmpty,

    xAccessor = barChartDefault.xAccessor,
    yAccessor = barChartDefault.yAccessor,
    xScale = barChartDefault.xScale,
    yScale = barChartDefault.yScale,
    maxYDomainForZeroData,
    minYForZeroData,

    barColor = barChartDefault.barColor,
    barSelectedColor = barChartDefault.barSelectedColor,
    margin,

    leftAxisProps = barChartDefault.leftAxisProps,
    bottomAxisProps = barChartDefault.bottomAxisProps,
    gridProps = barChartDefault.gridProps,

    popoverRenderer,
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

    const maxDataValue = useMaxDataValue(data, yAccessor);
    const adaptedYScale = useAdaptYScaleToZeroValues(yScale, maxDataValue, maxYDomainForZeroData);
    const adaptedYAccessor = useAdaptYAccessorToZeroValue(yAccessor, maxDataValue, minYForZeroData);

    const accessors = { xAccessor, yAccessor: adaptedYAccessor };

    const { computeNumTicks: computeLeftAxisNumTicks, ...mergedLeftAxisProps } = useMergedProps<AxisProps<DatumType>>(
        leftAxisProps,
        barChartDefault.leftAxisProps,
    );

    const { computeNumTicks: computeBottomAxisNumTicks, ...mergedBottomAxisProps } = useMergedProps<
        AxisProps<DatumType>
    >(bottomAxisProps, barChartDefault.bottomAxisProps);

    // In case of no data we should render empty graph with axises
    // but they don't render at all without any data.
    // To handle this case we will render the same graph with fake data and hide bars
    if (!data.length) {
        return <BarChart {...getMockedProps()} isEmpty />;
    }

    return (
        <ChartWrapper>
            <ParentSize>
                {({ width, height }) => {
                    return (
                        <XYChart
                            width={width}
                            height={height}
                            xScale={xScale}
                            yScale={adaptedYScale}
                            margin={internalMargin}
                            captureEvents={false}
                        >
                            {renderGradients?.()}

                            <Axis
                                orientation="left"
                                numTicks={computeLeftAxisNumTicks?.(width, height, internalMargin, data)}
                                {...mergedLeftAxisProps}
                            />

                            <Axis
                                orientation="bottom"
                                numTicks={computeBottomAxisNumTicks?.(width, height, internalMargin, data)}
                                {...mergedBottomAxisProps}
                            />

                            <line
                                x1={internalMargin.left}
                                x2={internalMargin.left}
                                y1={0}
                                y2={height - internalMargin.bottom}
                                stroke={gridProps?.stroke}
                            />

                            <Grid {...gridProps} />

                            <StyledBarSeries
                                as={BarSeries<AxisScale, AxisScale, DatumType>}
                                $hasSelectedItem={hasSelectedBar}
                                $color={barColor}
                                $selectedColor={barSelectedColor}
                                $isEmpty={isEmpty}
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
                                                defaultOpen
                                                placement="topLeft"
                                                key={`${xAccessor(tooltipData.nearestDatum.datum)}`}
                                                content={popoverRenderer?.(tooltipData.nearestDatum.datum)}
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
