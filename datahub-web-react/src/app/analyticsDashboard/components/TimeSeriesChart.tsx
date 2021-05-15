import React, { useMemo } from 'react';
import { XYChart, LineSeries, CrossHair, XAxis, YAxis } from '@data-ui/xy-chart';
import { scaleOrdinal } from '@vx/scale';

import {
    TimeSeriesChart as TimeSeriesChartType,
    DateInterval,
    NumericDataPoint,
    NamedLine,
} from '../../../types.generated';
import { lineColors } from './lineColors';
import Legend from './Legend';

type Props = {
    chartData: TimeSeriesChartType;
    width: number;
    height: number;
};

const MARGIN_SIZE = 32;

const INTERVAL_TO_SECONDS = {
    [DateInterval.Second]: 1,
    [DateInterval.Minute]: 60,
    [DateInterval.Hour]: 3600,
    [DateInterval.Day]: 86400,
    [DateInterval.Week]: 604800,
    [DateInterval.Month]: 2419200,
    [DateInterval.Year]: 31536000,
};

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

function extrapolatedPoints(chartData: TimeSeriesChartType) {
    const startDate = new Date(Number(chartData.dateRange.start));
    const endDate = new Date(Number(chartData.dateRange.end));
    const intervalMs = INTERVAL_TO_SECONDS[chartData.interval] * 1000;
    const returnLines: NamedLine[] = [];
    chartData.lines.forEach((line) => {
        const newLine = [...line.data];
        console.log(newLine, endDate);
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

export const TimeSeriesChart = ({ chartData, width, height }: Props) => {
    const ordinalColorScale = scaleOrdinal<string, string>({
        domain: chartData.lines.map((data) => data.name),
        range: lineColors.slice(0, chartData.lines.length),
    });

    const extrapolatedData = useMemo(() => extrapolatedPoints(chartData), [chartData]);

    return (
        <>
            <XYChart
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
                snapTooltipToDataX
            >
                <XAxis />
                <YAxis />
                {extrapolatedData.map((line, i) => (
                    <LineSeries
                        showPoints
                        data={line.data.map((point) => ({ x: new Date(point.x).getTime().toString(), y: point.y }))}
                        stroke={lineColors[i]}
                    />
                ))}
                <CrossHair showHorizontalLine={false} fullHeight stroke="pink" />
            </XYChart>
            <Legend ordinalScale={ordinalColorScale} />
        </>
    );
};
