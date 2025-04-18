import React from 'react';

import { Pie } from '@visx/shape';
import { PieArcDatum } from '@visx/shape/lib/shapes/Pie';
import { Annotation, Label, Connector } from '@visx/annotation';

import { useDataAnnotationPosition } from './usePieDataAnnotation';

const PieDataAnnotation = ({
    title,
    arc,
    path,
    subtitle,
}: {
    title: string;
    path: any;
    arc: PieArcDatum<{ [x: string]: string }>;
    subtitle?: string;
}) => {
    const { surfaceX, surfaceY, labelX, labelY } = useDataAnnotationPosition({ arc, path });

    return (
        <Annotation x={surfaceX} y={surfaceY} dx={labelX} dy={labelY}>
            <Connector type="elbow" className="dark-width" />
            <Label
                showBackground={false}
                showAnchorLine={false}
                title={title}
                titleFontWeight={600}
                titleFontSize={12}
                subtitle={subtitle}
                subtitleFontSize={10}
                width={85}
            />
        </Annotation>
    );
};

export const PieChart = ({ data }: any) => {
    const width = 380;
    const height = 280;
    const margin = { top: 50, right: 50, bottom: 50, left: 50 };

    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const radius = Math.min(innerWidth, innerHeight) * 0.45;
    const centerY = innerHeight / 2;
    const centerX = innerWidth / 2;

    const pieSortValues = (a, b) => b - a;
    const value = (d) => d.value;

    const createValueLabel = (count: number) => {
        let total = 0;
        data.forEach((d) => {
            total += d.value;
        });
        const percent = Math.round((count / total) * 100);
        return `${percent}% (${count.toLocaleString()})`;
    };

    return (
        <svg width={width} height={height}>
            <g transform={`translate(${centerX + margin.left}, ${centerY + margin.top})`}>
                <Pie data={data} pieValue={value} pieSortValues={pieSortValues} outerRadius={radius}>
                    {(pie) => {
                        return pie.arcs.map((arc) => {
                            const { name } = arc.data;
                            const { color } = arc.data;
                            const arcPath = pie.path(arc);
                            return (
                                <g key={`arc-${name}`}>
                                    <path d={arcPath || ''} fill={color} stroke="white" strokeWidth="2" />
                                    {arc.endAngle - arc.startAngle !== 0 ? (
                                        <PieDataAnnotation
                                            arc={arc}
                                            path={pie.path}
                                            title={arc.data.name}
                                            subtitle={createValueLabel(arc.value)}
                                        />
                                    ) : null}
                                </g>
                            );
                        });
                    }}
                </Pie>
            </g>
        </svg>
    );
};
