import React, { useCallback, useMemo, useState } from 'react';
import { colors } from '@src/alchemy-components/theme';
import { abbreviateNumber } from '@src/app/dataviz/utils';
import { TickLabelProps } from '@visx/axis';
import { LinearGradient } from '@visx/gradient';
import { ParentSize } from '@visx/responsive';
import { Axis, AxisScale, BarSeries, Grid, Tooltip, XYChart } from '@visx/xychart';
import dayjs from 'dayjs';
import { Popover } from '../Popover';
import { ChartWrapper, StyledBarSeries } from './components';
import { AxisProps, BarChartProps, ColorAccessor, Datum, GridProps, XAccessor, YAccessor } from './types';
import { getMockedProps } from './utils';
import useMergedProps from './hooks/useMergedProps';
import useAdaptYScaleToZeroValues from './hooks/useAdaptYScaleToZeroValues';
import useAdaptYAccessorToZeroValue from './hooks/useAdaptYAccessorToZeroValues';
import useMaxDataValue from './hooks/useMaxDataValue';
import { COLOR_SCHEME_TO_PARAMS, DEFAULT_COLOR_SCHEME, DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL } from './constants';
import TruncatableTick from './components/TruncatableTick';

const commonTickLabelProps: TickLabelProps<Datum> = {
    fontSize: 10,
    fontFamily: 'Mulish',
    fill: colors.gray[1700],
};

export const barChartDefault: BarChartProps = {
    data: [],

    xScale: { type: 'band', paddingInner: 0.4, paddingOuter: 0.1 },
    yScale: { type: 'linear', nice: true, round: true },

    leftAxisProps: {
        tickFormat: abbreviateNumber,
        tickLabelProps: {
            ...commonTickLabelProps,
            textAnchor: 'end',
            width: 50,
        },
        hideAxisLine: true,
        hideTicks: true,
    },
    maxLengthOfLeftAxisLabel: DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL,
    showLeftAxisLine: false,
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
};

export function BarChart({
    data,
    isEmpty,
    horizontal,

    xScale = barChartDefault.xScale,
    yScale = barChartDefault.yScale,
    maxYDomainForZeroData,
    minYForZeroData,

    margin,

    leftAxisProps = barChartDefault.leftAxisProps,
    maxLengthOfLeftAxisLabel: leftAxisLabelLimitOfChars = barChartDefault.maxLengthOfLeftAxisLabel,
    showLeftAxisLine = barChartDefault.showLeftAxisLine,
    bottomAxisProps = barChartDefault.bottomAxisProps,
    gridProps = barChartDefault.gridProps,

    popoverRenderer,
}: BarChartProps) {
    const [selectedBarIndex, setSelectedBarIndex] = useState<number | null>(null);
    const [howeredBarIndex, setHoweredBarIndex] = useState<number | null>(null);

    // FYI: additional margins to show left and bottom axises
    const internalMargin = {
        top: (margin?.top ?? 0) + 30,
        right: margin?.right ?? 0,
        bottom: (margin?.bottom ?? 0) + 35,
        left: (margin?.left ?? 0) + 50,
    };

    const xAccessor: XAccessor = (datum) => datum.x;
    const yAccessor: YAccessor = (datum) => datum.y;

    const maxDataValue = useMaxDataValue(data, yAccessor);
    const adaptedYScale = useAdaptYScaleToZeroValues(data, yScale, maxYDomainForZeroData);
    const adaptedYAccessor = useAdaptYAccessorToZeroValue(yAccessor, maxDataValue, minYForZeroData);

    const accessors = { xAccessor, yAccessor: adaptedYAccessor };

    const { computeNumTicks: computeLeftAxisNumTicks, ...mergedLeftAxisProps } = useMergedProps<AxisProps>(
        leftAxisProps,
        barChartDefault.leftAxisProps,
    );

    const { computeNumTicks: computeBottomAxisNumTicks, ...mergedBottomAxisProps } = useMergedProps<AxisProps>(
        bottomAxisProps,
        barChartDefault.bottomAxisProps,
    );

    const mergedGridProps = useMergedProps<GridProps>(gridProps, barChartDefault.gridProps);

    const gradientIdSuffix = useMemo(() => `bar${horizontal ? `-horizontal` : ''}`, [horizontal]);

    const colorAccessor: ColorAccessor = useCallback(
        (datum, index) => {
            if (isEmpty) return colors.transparent;
            const colorTheme = datum.colorScheme ?? DEFAULT_COLOR_SCHEME;
            const colorThemeParams = COLOR_SCHEME_TO_PARAMS[colorTheme];
            if (index === selectedBarIndex) return colorThemeParams.mainColor;
            if (index === howeredBarIndex) return colorThemeParams.mainColor;
            return `url(#${gradientIdSuffix}-${colorTheme})`;
        },
        [selectedBarIndex, howeredBarIndex, gradientIdSuffix, isEmpty],
    );

    const renderGradients = () => {
        const colorSchemes = [
            ...new Set([
                ...data.map((datum) => datum.colorScheme).filter((scheme) => scheme !== undefined),
                DEFAULT_COLOR_SCHEME,
            ]),
        ];

        return (
            <>
                {colorSchemes.map((colorScheme) => {
                    const colorSchemeParams = COLOR_SCHEME_TO_PARAMS[colorScheme ?? DEFAULT_COLOR_SCHEME];
                    const { mainColor } = colorSchemeParams;
                    const { alternativeColor } = colorSchemeParams;
                    const fromColor = horizontal ? alternativeColor : mainColor;
                    const toColor = horizontal ? mainColor : alternativeColor;

                    return (
                        <LinearGradient
                            id={`${gradientIdSuffix}-${colorScheme}`}
                            from={fromColor}
                            to={toColor}
                            vertical={!horizontal}
                            {...(horizontal ? { fromOpacity: 0.6 } : { toOpacity: 0.6 })}
                        />
                    );
                })}
            </>
        );
    };

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
                            horizontal={horizontal}
                        >
                            {renderGradients()}

                            <Axis
                                orientation="left"
                                numTicks={computeLeftAxisNumTicks?.(width, height, internalMargin, data)}
                                tickComponent={(props) => (
                                    <TruncatableTick {...props} limit={leftAxisLabelLimitOfChars} />
                                )}
                                {...mergedLeftAxisProps}
                            />

                            <Axis
                                orientation="bottom"
                                numTicks={computeBottomAxisNumTicks?.(width, height, internalMargin, data)}
                                {...mergedBottomAxisProps}
                            />

                            <Grid {...mergedGridProps} />

                            {/* hide the first (left) column line */}
                            {mergedGridProps.columns && (
                                <line
                                    x1={internalMargin.left}
                                    x2={internalMargin.left}
                                    y1={0}
                                    y2={height - internalMargin.bottom}
                                    stroke="white"
                                    strokeWidth={2}
                                />
                            )}

                            {showLeftAxisLine && (
                                <line
                                    x1={internalMargin.left}
                                    x2={internalMargin.left}
                                    y1={0}
                                    y2={height - internalMargin.bottom}
                                    stroke={mergedGridProps?.stroke}
                                />
                            )}

                            <StyledBarSeries
                                as={BarSeries<AxisScale, AxisScale, Datum>}
                                $hasSelectedItem={selectedBarIndex !== null}
                                $isEmpty={isEmpty}
                                dataKey="bar-seria-0"
                                data={data}
                                radius={4}
                                radiusTop
                                radiusBottom={horizontal}
                                onBlur={() => setSelectedBarIndex(null)}
                                onFocus={({ index }) => setSelectedBarIndex(index)}
                                colorAccessor={colorAccessor}
                                onPointerMove={({ index }) => setHoweredBarIndex(index)}
                                onPointerOut={() => setHoweredBarIndex(null)}
                                {...accessors}
                            />

                            <Tooltip<Datum>
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
