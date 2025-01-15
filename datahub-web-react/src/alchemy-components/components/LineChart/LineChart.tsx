import { colors } from '@src/alchemy-components/theme';
// import { abbreviateNumber } from '@src/app/dataviz/utils';
import { TickLabelProps } from '@visx/axis';
import { curveMonotoneX } from '@visx/curve';
import { LinearGradient } from '@visx/gradient';
import { ParentSize } from '@visx/responsive';
import { AreaSeries, Axis, AxisScale, Grid, LineSeries, Tooltip, XYChart } from '@visx/xychart';
import dayjs from 'dayjs';
import React, { useState } from 'react';
import { Popover } from '../Popover';
import { ChartWrapper } from './components';
import { LineChartProps } from './types';
import { abbreviateNumber } from '../dataviz/utils';

const commonTickLabelProps: TickLabelProps<any> = {
    fontSize: 10,
    fontFamily: 'Mulish',
    fill: colors.gray[1700],
};

const GLYPH_DROP_SHADOW_FILTER = `
    drop-shadow(0px 1px 3px rgba(33, 23, 95, 0.30))
    drop-shadow(0px 2px 5px rgba(33, 23, 95, 0.25))
    drop-shadow(0px -2px 5px rgba(33, 23, 95, 0.25)
`;

export const lineChartDefault: LineChartProps<any> = {
    data: [],
    xAccessor: (datum) => datum?.x,
    yAccessor: (datum) => datum?.y,
    leftAxisTickFormat: abbreviateNumber,
    leftAxisTickLabelProps: {
        ...commonTickLabelProps,
        textAnchor: 'end',
    },
    bottomAxisTickFormat: (x) => dayjs(x).format('D MMM'),
    bottomAxisTickLabelProps: {
        ...commonTickLabelProps,
        textAnchor: 'middle',
        verticalAnchor: 'start',
    },
    lineColor: colors.violet[500],
    areaColor: 'url(#line-gradient)',
    gridColor: '#e0e0e0',
    renderGradients: () => (
        <LinearGradient id="line-gradient" from={colors.violet[200]} to={colors.white} toOpacity={0.6} />
    ),
    toolbarVerticalCrosshairStyle: {
        stroke: colors.white,
        strokeWidth: 2,
        filter: GLYPH_DROP_SHADOW_FILTER,
    },
    renderTooltipGlyph: (props) => {
        return (
            <>
                <circle cx={props.x} cy={props.y} r="8" fill={colors.white} filter={GLYPH_DROP_SHADOW_FILTER} />
                <circle cx={props.x} cy={props.y} r="6" fill={colors.violet[500]} />
            </>
        );
    },
};

export function LineChart<DatumType extends object>({
    data,
    xAccessor = lineChartDefault.xAccessor,
    yAccessor = lineChartDefault.yAccessor,
    renderTooltipContent,
    margin,
    leftAxisTickFormat = lineChartDefault.leftAxisTickFormat,
    leftAxisTickLabelProps = lineChartDefault.leftAxisTickLabelProps,
    bottomAxisTickFormat = lineChartDefault.bottomAxisTickFormat,
    bottomAxisTickLabelProps = lineChartDefault.bottomAxisTickLabelProps,
    lineColor = lineChartDefault.lineColor,
    areaColor = lineChartDefault.areaColor,
    gridColor = lineChartDefault.gridColor,
    renderGradients = lineChartDefault.renderGradients,
    toolbarVerticalCrosshairStyle = lineChartDefault.toolbarVerticalCrosshairStyle,
    renderTooltipGlyph = lineChartDefault.renderTooltipGlyph,
}: LineChartProps<DatumType>) {
    const [showGrid, setShowGrid] = useState<boolean>(false);

    // FYI: additional margins to show left and bottom axises
    const internalMargin = {
        top: (margin?.top ?? 0) + 30,
        right: (margin?.right ?? 0) + 20,
        bottom: (margin?.bottom ?? 0) + 35,
        left: (margin?.left ?? 0) + 40,
    };

    const accessors = { xAccessor, yAccessor };

    return (
        <ChartWrapper onMouseEnter={() => setShowGrid(true)} onMouseLeave={() => setShowGrid(false)}>
            <ParentSize>
                {({ width, height }) => {
                    return (
                        <XYChart
                            width={width}
                            height={height}
                            xScale={{ type: 'time' }}
                            yScale={{ type: 'linear', nice: true, round: true }}
                            margin={internalMargin}
                            captureEvents
                        >
                            {renderGradients?.()}

                            <Axis
                                orientation="left"
                                tickFormat={leftAxisTickFormat}
                                tickLabelProps={leftAxisTickLabelProps}
                                hideAxisLine
                                hideTicks
                            />

                            <Axis
                                orientation="bottom"
                                numTicks={Math.floor(data.length / 2)}
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

                            {showGrid && (
                                <Grid rows={false} columns stroke={gridColor} numTicks={data.length} lineStyle={{}} />
                            )}

                            <AreaSeries<AxisScale, AxisScale, DatumType>
                                dataKey="line-chart-seria-01"
                                data={data}
                                fill={areaColor}
                                curve={curveMonotoneX}
                                {...accessors}
                            />
                            <LineSeries<AxisScale, AxisScale, DatumType>
                                dataKey="line-chart-seria-01"
                                data={data}
                                stroke={lineColor}
                                curve={curveMonotoneX}
                                {...accessors}
                            />

                            <Tooltip<DatumType>
                                snapTooltipToDatumX
                                snapTooltipToDatumY
                                showVerticalCrosshair
                                applyPositionStyle
                                showSeriesGlyphs
                                verticalCrosshairStyle={toolbarVerticalCrosshairStyle}
                                renderGlyph={renderTooltipGlyph}
                                unstyled
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
