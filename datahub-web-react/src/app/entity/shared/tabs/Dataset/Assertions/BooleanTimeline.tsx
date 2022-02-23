import React, { useMemo } from 'react';
import { Bar } from '@vx/shape';
import { Group } from '@vx/group';
import { AxisBottom } from '@vx/axis';
import { scaleTime } from 'd3-scale';

export type BooleanResult = {
    result: boolean;
    title: React.ReactNode;
    content: React.ReactNode;
};

export type BooleanDataPoint = {
    time: number;
    result: BooleanResult;
};

export type TimeRange = {
    startMs: number;
    endMs: number;
};

type Props = {
    data: Array<BooleanDataPoint>;
    timeRange: TimeRange;
};

export const BooleanTimeline = ({ data, timeRange }: Props) => {
    console.log(timeRange);

    // const yMax = 40;

    // scales, memoize for performance
    const xScale = useMemo(() => scaleTime<number>([0, Date.now()]), []);
    const transformedData = data.map((result, i) => {
        return {
            index: i,
            title: result.result.title,
            result: result.result.result,
            timestamp: result.time,
        };
    });

    return (
        <>
            <svg width={500} height={100}>
                <rect x={0} y={0} width={250} height={60} fill="black" rx={14} />
                <Group top={8} left={8}>
                    {transformedData.map((d) => {
                        // const title = d.title;
                        // const result = d.result;
                        const { index } = d;
                        const barWidth = 7;
                        const barHeight = 13;
                        // const barX = xScale(d.timestamp);
                        // const barY = yMax - barHeight;
                        return (
                            <Bar
                                key={`bar-${index}`}
                                x={0}
                                y={0}
                                width={barWidth}
                                height={barHeight}
                                fill="white"
                                onClick={() => {
                                    console.log('Fuck off');
                                }}
                            />
                        );
                    })}
                </Group>
                <AxisBottom
                    top={150}
                    left={12}
                    scale={xScale}
                    tickLabelProps={(_) => ({
                        fontSize: 11,
                        textAnchor: 'start',
                        angle: 40,
                    })}
                />
            </svg>
        </>
    );
};
