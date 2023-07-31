import React, { useMemo } from 'react';
import { XYChart, LineSeries, CrossHair, XAxis, YAxis } from '@data-ui/xy-chart';
import { scaleOrdinal } from '@vx/scale';
import { TimeSeriesChart as TimeSeriesChartType, NumericDataPoint, NamedLine } from '../../../types.generated';
import { lineColors } from './lineColors';
import Legend from './Legend';
import { addInterval } from '../../shared/time/timeUtils';
import { formatNumber } from '../../shared/formatNumber';

type ScaleConfig = {
    type: 'time' | 'timeUtc' | 'linear' | 'band' | 'ordinal';
    includeZero?: boolean;
};

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
    yScale?: ScaleConfig;
    yAxis?: AxisConfig;
};

const MARGIN = {
    TOP: 40,
    RIGHT: 45,
    BOTTOM: 40,
    LEFT: 40,
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
                eventTrigger="container"
                ariaLabel={chartData.title}
                width={width}
                height={height}
                margin={{ top: MARGIN.TOP, right: MARGIN.RIGHT, bottom: MARGIN.BOTTOM, left: MARGIN.LEFT }}
                xScale={{ type: 'time' }}
                yScale={
                    yScale ?? {
                        type: 'linear',
                    }
                }
                renderTooltip={({ datum }) => (
                    <div>
                        <div>{new Date(Number(datum.x)).toDateString()}</div>
                        <div>{datum.y}</div>
                    </div>
                )}
                snapTooltipToDataX={false}
            >
                <XAxis axisStyles={{ stroke: style && style.axisColor, strokeWidth: style && style.axisWidth }} />
                <YAxis
                    axisStyles={{ stroke: style && style.axisColor, strokeWidth: style && style.axisWidth }}
                    tickFormat={(tick) => (yAxis?.formatter ? yAxis.formatter(tick) : formatNumber(tick))}
                />
                {lines.map((line, i) => (
                    <LineSeries
                        showPoints
                        data={line.data.map((point) => ({ x: new Date(point.x).getTime().toString(), y: point.y }))}
                        stroke={(style && style.lineColor) || lineColors[i]}
                    />
                ))}
                <CrossHair
                    showHorizontalLine={false}
                    fullHeight
                    stroke={(style && style.crossHairLineColor) || '#D8D8D8'}
                />
            </XYChart>
            {!hideLegend && <Legend ordinalScale={ordinalColorScale} />}
        </>
    );
};
