import React, { useMemo } from 'react';
import { XYChart, LineSeries, CrossHair, XAxis, YAxis } from '@data-ui/xy-chart';
import { scaleOrdinal } from '@vx/scale';
import { TimeSeriesChart as TimeSeriesChartType, NumericDataPoint, NamedLine } from '../../../types.generated';
import { lineColors } from './lineColors';
import Legend from './Legend';
import { INTERVAL_TO_SECONDS } from '../../shared/time/timeUtils';
import { formatNumber } from '../../shared/formatNumber';

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
};

const MARGIN_SIZE = 40;

function insertBlankAt(ts: number, newLine: Array<NumericDataPoint>) {
    const dateString = new Date(ts).toString();
    for (let i = 0; i < newLine.length; i++) {
        if (new Date(newLine[i].x).getTime() > ts) {
            newLine.splice(i, 0, { x: dateString, y: 0 });
            return;
        }
    }
    newLine.push({ x: dateString, y: 0 });
}

function computeLines(chartData: TimeSeriesChartType, insertBlankPoints: boolean) {
    if (!insertBlankPoints) {
        return chartData.lines;
    }

    const startDate = new Date(Number(chartData.dateRange.start));
    const endDate = new Date(Number(chartData.dateRange.end));
    const intervalMs = INTERVAL_TO_SECONDS[chartData.interval] * 1000;
    const returnLines: NamedLine[] = [];
    chartData.lines.forEach((line) => {
        const newLine = [...line.data];
        for (let i = endDate.getTime(); i > startDate.getTime(); i -= intervalMs) {
            const pointOverlap = line.data.filter((point) => {
                return Math.abs(new Date(point.x).getTime() - i) > intervalMs;
            });
            if (pointOverlap) {
                break;
            }

            const pointGreater = line.data.find((point) => new Date(point.x).getTime() > i);
            if (pointGreater) {
                break;
            }
            insertBlankAt(i, newLine);
        }

        for (let i = startDate.getTime(); i <= endDate.getTime(); i += intervalMs) {
            const pointOverlap = line.data.find((point) => {
                return Math.abs(new Date(point.x).getTime() - i) <= intervalMs;
            });

            if (pointOverlap) {
                break;
            }

            const pointSmaller = line.data.find((point) => new Date(point.x).getTime() < i);
            if (pointSmaller) {
                break;
            }
            insertBlankAt(i, newLine);
        }

        returnLines.push({ name: line.name, data: newLine });
    });
    return returnLines;
}

export const TimeSeriesChart = ({ chartData, width, height, hideLegend, style, insertBlankPoints }: Props) => {
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
                margin={{ top: MARGIN_SIZE, right: MARGIN_SIZE, bottom: MARGIN_SIZE, left: MARGIN_SIZE }}
                xScale={{ type: 'time' }}
                yScale={{ type: 'linear' }}
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
                    tickFormat={(tick) => formatNumber(tick)}
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
