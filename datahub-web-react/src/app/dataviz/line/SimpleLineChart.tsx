import React from 'react';

import { curveCatmullRom } from '@visx/curve';
import { LinePath } from '@visx/shape';
import { ParentSize } from '@visx/responsive';
import { MarkerCircle } from '@visx/marker';
import { extent, max } from '@visx/vendor/d3-array';
import { scaleTime, scaleLinear } from '@visx/scale';

import { ChartWrapper } from '../components';

interface Props {
    data: any;
}

type Data = {
    date: string;
    value: number;
};

export const SimpleLineChart = ({ data }: Props) => {
    const getDate = (d: Data) => new Date(d.date);
    const getValue = (d: Data) => d.value;

    const markerEnd = 'url(#marker-circle)';

    return (
        <ChartWrapper>
            <ParentSize>
                {({ width }) => {
                    if (!width) return null;

                    const height = 20;

                    const xScale = scaleTime<number>({
                        range: [0, width - 10],
                        domain: extent(data, getDate) as [Date, Date],
                    });

                    const yScale = scaleLinear<number>({
                        range: [height / 2, 5],
                        domain: [0, (max(data, getValue) || 0) as number],
                        nice: true,
                    });

                    return (
                        <svg width={width} height={height}>
                            <defs>
                                <linearGradient id="gradient" x1="0%" y1="0%" x2="100%" y2="100%">
                                    <stop offset="0%" stopColor="#20D3BD" />
                                    <stop offset="100%" stopColor="#9F33CC" />
                                </linearGradient>
                            </defs>
                            <MarkerCircle id="marker-circle" fill="#9F33CC" stroke="#fff" strokeWidth={2} size={3} />
                            <LinePath
                                data={data}
                                x={(d: Data) => xScale(getDate(d))}
                                y={(d: Data) => yScale(getValue(d))}
                                curve={curveCatmullRom}
                                markerEnd={markerEnd}
                                stroke="#9F33CC"
                            />
                        </svg>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
};
