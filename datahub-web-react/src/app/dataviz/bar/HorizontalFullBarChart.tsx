/* eslint-disable @typescript-eslint/dot-notation, no-param-reassign */

import React from 'react';

import { Grid, XYChart } from '@visx/xychart';
import { BarStackHorizontal, Bar } from '@visx/shape';
import { AxisLeft } from '@visx/axis';
import { scaleLinear, scaleBand } from '@visx/scale';
import { ParentSize } from '@visx/responsive';

import { Legend } from '../Legend';
import { ChartWrapper } from '../components';

import { COMPLETED_COLOR, NOT_STARTED_COLOR, IN_PROGRESS_COLOR } from '../constants';

export const HorizontalFullBarChart = <Data extends object, DataKeys>({
    data,
    dataKeys,
    yAccessor,
    colorAccessor,
}: {
    data: Data[];
    dataKeys: DataKeys;
    yAccessor: (d: Data) => string;
    colorAccessor: (d: string) => string;
}) => {
    if (!data || !data.length || !dataKeys) return null;
    if (!Array.isArray(dataKeys)) throw new Error('Datakeys must be an array');

    const totals = data.reduce((allTotals, currentDate) => {
        const total = dataKeys.reduce((dailyTotal, k) => {
            dailyTotal += Number(currentDate[k]);
            return dailyTotal;
        }, 0);
        allTotals.push(total);
        return allTotals;
    }, [] as number[]);

    const xScale = scaleLinear<number>({
        domain: [0, Math.max(...totals)],
        nice: true,
    });

    const yScale = scaleBand<string>({
        domain: data.map(yAccessor),
        padding: 0.2,
    });

    return (
        <ChartWrapper>
            <ParentSize>
                {({ width: parentWidth }) => {
                    if (!parentWidth) return null;

                    const margin = { top: 20, right: 0, bottom: 0, left: 120 };
                    const baseHeight = 280;
                    const calculatedHeight = data.length > 4 ? baseHeight + data.length * 10 : baseHeight;
                    const xMax = parentWidth - margin.left - margin.right;
                    const yMax = calculatedHeight - margin.top - margin.bottom;

                    xScale.rangeRound([0, xMax]);
                    yScale.rangeRound([yMax, 0]);

                    return (
                        <>
                            <XYChart
                                width={parentWidth}
                                height={baseHeight}
                                margin={margin}
                                xScale={{ type: 'linear' }}
                                yScale={{ type: 'band', paddingInner: 0.3 }}
                                captureEvents={false}
                            >
                                <Grid numTicks={5} lineStyle={{ stroke: '#EAEAEA' }} rows={false} />

                                <BarStackHorizontal
                                    data={data}
                                    keys={dataKeys.slice().reverse()}
                                    width={parentWidth}
                                    y={yAccessor}
                                    xScale={xScale}
                                    yScale={yScale}
                                    color={colorAccessor}
                                >
                                    {(barStacks) =>
                                        barStacks.map((barStack) =>
                                            barStack.bars.map((bar) => {
                                                // Use the bar color to determine which label to display for Doc Initiatives
                                                let label = null;
                                                if (bar.color === COMPLETED_COLOR) label = bar.bar.data['Completed'];
                                                if (bar.color === IN_PROGRESS_COLOR)
                                                    label = bar.bar.data['In Progress'];
                                                if (bar.color === NOT_STARTED_COLOR)
                                                    label = bar.bar.data['Not Started'];

                                                if (label === '0') return null;

                                                const { x, y, width, height } = bar;
                                                const { left } = margin;

                                                const newX = x + left + 10;
                                                const barWidth = width + 5;

                                                let textX = barWidth <= 20 ? newX + barWidth - 5 : newX + barWidth - 10;
                                                if (barWidth > 300) textX = newX + barWidth - 20;

                                                return (
                                                    <g key={`barstack-horizontal-${barStack.index}-${bar.index}`}>
                                                        <Bar
                                                            x={newX}
                                                            y={y}
                                                            width={barWidth}
                                                            height={height}
                                                            fill={bar.color}
                                                        />
                                                        {label && (
                                                            <text
                                                                className="horizontalBarChartInlineLabel"
                                                                x={textX}
                                                                y={y + height / 2}
                                                                dy=".33em"
                                                                textAnchor="end"
                                                                fontSize={11}
                                                            >
                                                                {label}
                                                            </text>
                                                        )}
                                                    </g>
                                                );
                                            }),
                                        )
                                    }
                                </BarStackHorizontal>
                                <AxisLeft
                                    hideAxisLine
                                    scale={yScale}
                                    tickLabelProps={{
                                        fontSize: 11,
                                        textAnchor: 'end',
                                        dy: '0.33em',
                                        x: margin.left,
                                        width: 135,
                                    }}
                                    tickLineProps={{
                                        x1: margin.left + 5,
                                        x2: margin.left + 10,
                                    }}
                                />
                            </XYChart>
                            <Legend scale={colorAccessor} />
                        </>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
};
