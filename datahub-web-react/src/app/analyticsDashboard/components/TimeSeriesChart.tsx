import React, { useMemo } from 'react';
import styled from 'styled-components';
import { AxisScaleOutput } from '@visx/axis';
import { Axis, LineSeries, XYChart, Tooltip, GlyphSeries } from '@visx/xychart';
import { curveMonotoneX } from '@visx/curve';
import { ScaleConfig, scaleOrdinal } from '@visx/scale';
import { TimeSeriesChart as TimeSeriesChartType, NumericDataPoint, NamedLine } from '../../../types.generated';
import { lineColors } from './lineColors';
import Legend from './Legend';
import { addInterval } from '../../shared/time/timeUtils';
import { formatNumber } from '../../shared/formatNumber';

type AxisConfig = {
    formatter: (tick: number) => string;
};

type Props = {
    chartData: TimeSeriesChartType;
    width: number;
    height: number;
    hideLegend?: boolean;
    style?: {
        axisColor?: string;
        axisWidth?: number;
        lineColor?: string;
        lineWidth?: string;
        crossHairLineColor?: string;
    };
    insertBlankPoints?: boolean;
    yScale?: ScaleConfig<AxisScaleOutput, any, any>;
    yAxis?: AxisConfig;
};

const StyledTooltip = styled(Tooltip)`
    font-family: inherit !important;
    font-weight: 400 !important;
`;

const MARGIN = {
    TOP: 40,
    RIGHT: 45,
    BOTTOM: 40,
    LEFT: 40,
};

const accessors = {
    xAccessor: (d) => d.x,
    yAccessor: (d) => d.y,
};

function insertBlankAt(ts: number, newLine: Array<NumericDataPoint>) {
    const dateString = new Date(ts).toISOString();
    for (let i = 0; i < newLine.length; i++) {
        if (new Date(newLine[i].x).getTime() > ts) {
            newLine.splice(i, 0, { x: dateString, y: 0 });
            return;
        }
    }
    newLine.push({ x: dateString, y: 0 });
}

export function computeLines(chartData: TimeSeriesChartType, insertBlankPoints: boolean) {
    if (!insertBlankPoints) {
        return chartData.lines;
    }

    const startDate = new Date(Number(chartData.dateRange.start));
    const endDate = new Date(Number(chartData.dateRange.end));
    const returnLines: NamedLine[] = [];
    chartData.lines.forEach((line) => {
        const newLine = [...line.data];
        for (let i = startDate; i <= endDate; i = addInterval(1, i, chartData.interval)) {
            const pointOverlap = line.data.filter((point) => {
                return Math.abs(new Date(point.x).getTime() - i.getTime()) === 0;
            });

            if (pointOverlap.length === 0) {
                insertBlankAt(i.getTime(), newLine);
            }
        }

        returnLines.push({ name: line.name, data: newLine });
    });
    return returnLines;
}

const formatAxisDate = (value: number, chartData: TimeSeriesChartType) => {
    const date = new Date(value);

    switch (chartData.interval) {
        case 'MONTH':
            return date.toLocaleDateString('en-US', {
                month: 'short',
                year: 'numeric',
                timeZone: 'UTC',
            });
        case 'WEEK':
            return date.toLocaleDateString('en-US', {
                month: 'short',
                day: 'numeric',
                timeZone: 'UTC',
            });
        case 'DAY':
            return date.toLocaleDateString('en-US', {
                weekday: 'short',
                day: 'numeric',
                timeZone: 'UTC',
            });
        default:
            return date.toLocaleDateString('en-US', {
                month: 'short',
                day: 'numeric',
                year: 'numeric',
                timeZone: 'UTC',
            });
    }
};

export const TimeSeriesChart = ({
    chartData,
    width,
    height,
    hideLegend,
    style,
    insertBlankPoints,
    yScale,
    yAxis,
}: Props) => {
    const ordinalColorScale = scaleOrdinal<string, string>({
        domain: chartData.lines.map((data) => data.name),
        range: lineColors.slice(0, chartData.lines.length),
    });

    const lines = useMemo(() => computeLines(chartData, insertBlankPoints || false), [chartData, insertBlankPoints]);

    return (
        <>
            <XYChart
                accessibilityLabel={chartData.title}
                width={width}
                height={height}
                margin={{ top: MARGIN.TOP, right: MARGIN.RIGHT, bottom: MARGIN.BOTTOM, left: MARGIN.LEFT }}
                xScale={{ type: 'time' }}
                yScale={yScale ?? { type: 'linear' }}
            >
                <Axis
                    orientation="bottom"
                    stroke={style?.axisColor}
                    strokeWidth={style?.axisWidth}
                    tickLabelProps={{ fill: 'black', fontFamily: 'inherit', fontSize: 10 }}
                    numTicks={3}
                    tickFormat={(value) => formatAxisDate(value, chartData)}
                />
                <Axis
                    orientation="right"
                    stroke={style?.axisColor}
                    strokeWidth={style?.axisWidth}
                    tickFormat={(tick) => (yAxis?.formatter ? yAxis.formatter(tick) : formatNumber(tick))}
                    tickLabelProps={{ fill: 'black', fontFamily: 'inherit', fontSize: 10 }}
                    numTicks={3}
                />
                {lines.map((line, i) => (
                    <>
                        <LineSeries
                            dataKey={line.name}
                            data={line.data.map((point) => ({ x: new Date(point.x), y: point.y }))}
                            stroke={(style && style.lineColor) || lineColors[i]}
                            curve={curveMonotoneX}
                            {...accessors}
                        />
                        <GlyphSeries
                            dataKey={line.name}
                            data={line.data.map((point) => ({ x: new Date(point.x), y: point.y }))}
                            {...accessors}
                        />
                    </>
                ))}
                <StyledTooltip
                    snapTooltipToDatumX
                    showVerticalCrosshair
                    showDatumGlyph
                    verticalCrosshairStyle={{ stroke: '#D8D8D8', strokeDasharray: '5,2', strokeWidth: 1 }}
                    renderTooltip={({ tooltipData }) =>
                        tooltipData?.nearestDatum && (
                            <div>
                                <div>
                                    {formatAxisDate(accessors.xAccessor(tooltipData.nearestDatum.datum), chartData)}
                                </div>
                                <div>{accessors.yAccessor(tooltipData.nearestDatum.datum)}</div>
                            </div>
                        )
                    }
                />
            </XYChart>
            {!hideLegend && <Legend ordinalScale={ordinalColorScale} />}
        </>
    );
};
