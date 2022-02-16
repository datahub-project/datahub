import React, { useMemo } from 'react';
import { BarStack } from '@vx/shape';
import { scaleOrdinal, scaleLinear, scaleBand } from '@vx/scale';
import { Group } from '@vx/group';
import { AxisBottom, AxisRight } from '@vx/axis';

import { BarChart as BarChartType } from '../../../types.generated';
import { lineColors } from './lineColors';
import Legend from './Legend';

type Props = {
    chartData: BarChartType;
    width: number;
    height: number;
};

const WIDTH_MARGIN_SIZE = 55;
const HEIGHT_MARGIN_SIZE = 32;

function transformName(label: string) {
    if (label === 'DATA_JOB') {
        return 'TASK';
    }
    if (label === 'DATA_FLOW') {
        return 'PIPELINE';
    }
    return label;
}

function transformChartData(chartData: BarChartType) {
    return chartData.bars.map((bar, i) => {
        const name = transformName(bar.name);
        return {
            index: i,
            name,
            displayName: name.length > 15 ? `${name.substring(0, Math.min(15, name.length))}...` : name,
            ...bar.segments.reduce(
                (obj, segment) => ({
                    ...obj,
                    [segment.label]: segment.value,
                }),
                {},
            ),
        };
    });
}

export const BarChart = ({ chartData, width, height }: Props) => {
    const keys = useMemo(
        () =>
            chartData.bars
                .flatMap((bar) => bar.segments.map((segment) => segment.label))
                .filter((x, i, a) => a.indexOf(x) === i),
        [chartData],
    );
    const totals = useMemo(
        () => chartData.bars.map((bar) => bar.segments.reduce((total, segment) => total + segment.value, 0)),
        [chartData],
    );

    const segmentScale = scaleOrdinal<string, string>({
        domain: keys,
        range: lineColors.slice(0, keys.length),
    });

    const transformedChartData = useMemo(() => transformChartData(chartData), [chartData]);

    const yAxisScale = scaleLinear<number>({
        domain: [0, Math.max(...totals) * 1.1],
    });

    const xAxisScale = scaleBand<string>({
        domain: transformedChartData.map((bar) => bar.displayName),
        padding: 0.2,
    });

    const xMax = width - WIDTH_MARGIN_SIZE;
    const yMax = height - HEIGHT_MARGIN_SIZE - 80;

    xAxisScale.rangeRound([0, xMax]);
    yAxisScale.range([yMax, 0]);

    return (
        <>
            <svg width={width + WIDTH_MARGIN_SIZE} height={height}>
                <rect x={0} y={0} width={width} height={height} fill="white" rx={14} />
                <Group top={HEIGHT_MARGIN_SIZE} left={WIDTH_MARGIN_SIZE}>
                    <BarStack<typeof transformedChartData[0], typeof keys[number]>
                        data={transformedChartData}
                        keys={keys}
                        x={(data) => data.displayName}
                        xScale={xAxisScale}
                        yScale={yAxisScale}
                        color={segmentScale}
                    >
                        {(barStacks) => {
                            return barStacks.map((barStack) =>
                                barStack.bars
                                    .filter((bar) => !Number.isNaN(bar.bar[1]))
                                    .map((bar) => (
                                        <rect
                                            key={`bar-stack-${barStack.index}-${bar.index}`}
                                            x={bar.x}
                                            y={bar.y}
                                            height={bar.height}
                                            width={bar.width}
                                            fill={bar.color}
                                        >
                                            <title>
                                                {barStacks.length === 1
                                                    ? `${transformedChartData[bar.index].name}, ${
                                                          bar.bar[1] - bar.bar[0]
                                                      }`
                                                    : `${transformedChartData[bar.index].name}, ${bar.key}, ${
                                                          bar.bar[1] - bar.bar[0]
                                                      }`}
                                            </title>
                                        </rect>
                                    )),
                            );
                        }}
                    </BarStack>
                </Group>
                <AxisBottom
                    top={yMax + HEIGHT_MARGIN_SIZE}
                    left={WIDTH_MARGIN_SIZE}
                    scale={xAxisScale}
                    tickLabelProps={(_) => ({
                        fontSize: 11,
                        textAnchor: 'start',
                        angle: 40,
                    })}
                />
                <AxisRight
                    labelOffset={1000}
                    numTicks={5}
                    top={HEIGHT_MARGIN_SIZE}
                    left={xMax + WIDTH_MARGIN_SIZE}
                    scale={yAxisScale}
                    tickLabelProps={() => ({
                        fontSize: 10,
                        dx: '3px',
                        dy: '1px',
                        textAnchor: 'start',
                    })}
                />
            </svg>
            <Legend ordinalScale={segmentScale} />
        </>
    );
};
