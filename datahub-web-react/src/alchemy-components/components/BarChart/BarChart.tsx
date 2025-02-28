import React, { useCallback, useMemo, useState } from 'react';
import { colors } from '@src/alchemy-components/theme';
import { LinearGradient } from '@visx/gradient';
import { ParentSize } from '@visx/responsive';
import { Axis, AxisScale, BarSeries, Grid, Tooltip, XYChart } from '@visx/xychart';
import { Popover } from '../Popover';
import { ChartWrapper, StyledBarSeries } from './components';
import { AxisProps, BarChartProps, ColorAccessor, Datum, GridProps, XAccessor, YAccessor } from './types';
import { getMockedProps } from './utils';
import useMergedProps from './hooks/useMergedProps';
import usePrepareScales from './hooks/usePrepareScales';
import usePrepareAccessors from './hooks/usePrepareAccessors';
import { COLOR_SCHEME_TO_PARAMS, DEFAULT_COLOR_SCHEME } from './constants';
import TruncatableTick from './components/TruncatableTick';
import { barChartDefault } from './defaults';
import LeftAxisMarginSetter from './components/LeftAxisMarginSetter';
import { abbreviateNumber } from '../dataviz/utils';

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
    maxLengthOfLeftAxisLabel = barChartDefault.maxLengthOfLeftAxisLabel,
    showLeftAxisLine = barChartDefault.showLeftAxisLine,
    bottomAxisProps = barChartDefault.bottomAxisProps,
    gridProps = barChartDefault.gridProps,

    popoverRenderer,
}: BarChartProps) {
    const [selectedBarIndex, setSelectedBarIndex] = useState<number | null>(null);
    const [howeredBarIndex, setHoweredBarIndex] = useState<number | null>(null);
    const [leftAxisMargin, setLeftAxisMargin] = useState<number>(0);

    // FYI: additional margins to show left and bottom axises
    const internalMargin = useMemo(
        () => ({
            top: (margin?.top ?? 0) + 30,
            right: (margin?.right ?? 0) + 0,
            bottom: (margin?.bottom ?? 0) + 35,
            left: (margin?.left ?? 0) + leftAxisMargin + 6,
        }),
        [leftAxisMargin, margin],
    );

    const xAccessor: XAccessor = (datum) => datum.x;
    const yAccessor: YAccessor = (datum) => datum.y;
    const accessors = usePrepareAccessors(data, !!horizontal, xAccessor, yAccessor, minYForZeroData);
    const scales = usePrepareScales(data, !!horizontal, xScale, xAccessor, yScale, yAccessor, maxYDomainForZeroData);

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

            const isInversed = (horizontal ? accessors.xAccessor(datum) : accessors.yAccessor(datum)) < 0;

            return `url(#${gradientIdSuffix}-${colorTheme}${isInversed ? '-inversed' : ''})`;
        },
        [selectedBarIndex, howeredBarIndex, gradientIdSuffix, isEmpty, accessors, horizontal],
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
                    const gradientId = `${gradientIdSuffix}-${colorScheme}`;
                    const gradientInversedId = `${gradientId}-inversed`;

                    return (
                        <>
                            <LinearGradient
                                key={gradientId}
                                id={gradientId}
                                from={fromColor}
                                to={toColor}
                                vertical={!horizontal}
                                {...(horizontal ? { fromOpacity: 0.6 } : { toOpacity: 0.6 })}
                            />
                            <LinearGradient
                                key={gradientInversedId}
                                id={gradientInversedId}
                                from={toColor}
                                to={fromColor}
                                vertical={!horizontal}
                                {...(!horizontal ? { fromOpacity: 0.6 } : { toOpacity: 0.6 })}
                            />
                        </>
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
                            margin={internalMargin}
                            captureEvents={false}
                            horizontal={horizontal}
                            {...scales}
                        >
                            {renderGradients()}

                            <Axis
                                orientation="left"
                                numTicks={computeLeftAxisNumTicks?.(width, height, internalMargin, data)}
                                tickComponent={(props) => (
                                    <TruncatableTick {...props} limit={maxLengthOfLeftAxisLabel} />
                                )}
                                {...mergedLeftAxisProps}
                            />
                            <LeftAxisMarginSetter
                                setLeftMargin={setLeftAxisMargin}
                                formatter={leftAxisProps?.tickFormat ?? abbreviateNumber}
                                maxMargin={45}
                                numOfTicks={computeLeftAxisNumTicks?.(width, height, internalMargin, data)}
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
                                // needed for bounds to update correctly (https://airbnb.io/visx/tooltip)
                                key={Math.random()}
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
                                                // adjust offset for horizontal barchart to prevent blinking of popover and hover state
                                                align={horizontal ? { offset: [0, 20] } : undefined}
                                                placement={horizontal ? 'bottomRight' : 'topLeft'}
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
