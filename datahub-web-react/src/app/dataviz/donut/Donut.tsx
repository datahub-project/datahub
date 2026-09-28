import { Group } from '@visx/group';
import { Pie } from '@visx/shape';
import React, { useMemo } from 'react';
import { useTheme } from 'styled-components';

type Props = {
    /** Progress value, 0–100. */
    value: number;
    /** Outer diameter in pixels. */
    size?: number;
    /** Ring thickness in pixels. */
    thickness?: number;
    /** Optional content rendered in the center. Defaults to `${value}%`. */
    children?: React.ReactNode;
    /** Accessible label for the donut. */
    ariaLabel?: string;
};

const DEFAULT_SIZE = 96;
const DEFAULT_THICKNESS = 10;

export function Donut({ value, size = DEFAULT_SIZE, thickness = DEFAULT_THICKNESS, children, ariaLabel }: Props) {
    const theme = useTheme();
    const gradientId = useMemo(() => `donut-gradient-${Math.random().toString(36).slice(2, 10)}`, []);
    const radius = size / 2;
    const safeValue = Math.max(0, Math.min(100, value));

    const arcs = [
        { key: 'value', value: safeValue, fill: `url(#${gradientId})` },
        { key: 'rest', value: 100 - safeValue, fill: theme.colors.bgSurface },
    ];

    return (
        <svg
            width={size}
            height={size}
            role="img"
            aria-label={ariaLabel ?? `${Math.round(safeValue)} percent`}
            style={{ overflow: 'visible' }}
        >
            <defs>
                <linearGradient id={gradientId} x1={size} y1={0} x2={0} y2={0} gradientUnits="userSpaceOnUse">
                    <stop offset="0%" stopColor={theme.colors.iconBrand} />
                    <stop offset="100%" stopColor={theme.colors.borderBrand} />
                </linearGradient>
            </defs>
            <Group top={radius} left={radius}>
                <Pie
                    data={arcs}
                    pieValue={(d) => d.value}
                    outerRadius={radius}
                    innerRadius={radius - thickness}
                    cornerRadius={3}
                    padAngle={safeValue > 0 && safeValue < 100 ? 0.01 : 0}
                >
                    {(pie) =>
                        pie.arcs.map((arc) => (
                            <path key={arc.data.key} d={pie.path(arc) ?? undefined} fill={arc.data.fill} />
                        ))
                    }
                </Pie>
            </Group>
            <foreignObject x={0} y={0} width={size} height={size}>
                <div
                    style={{
                        width: size,
                        height: size,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                    }}
                >
                    {children ?? `${Math.round(safeValue)}%`}
                </div>
            </foreignObject>
        </svg>
    );
}
