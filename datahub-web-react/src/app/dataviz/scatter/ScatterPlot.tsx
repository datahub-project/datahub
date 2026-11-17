import { AxisBottom, AxisLeft } from '@visx/axis';
import { GridColumns, GridRows } from '@visx/grid';
import { Group } from '@visx/group';
import { ParentSize } from '@visx/responsive';
import { scaleLinear } from '@visx/scale';
import React, { useState } from 'react';
import styled, { useTheme } from 'styled-components';

export type ScatterPoint<T = unknown> = {
    id: string;
    x: number;
    y: number;
    label?: string;
    /** Optional fill for the circle. Defaults to brand color. */
    color?: string;
    meta?: T;
};

type Props<T> = {
    points: ScatterPoint<T>[];
    /** Domain for the X axis. Defaults to [0, 100]. */
    xDomain?: [number, number];
    /** Domain for the Y axis. Defaults to [0, 100]. */
    yDomain?: [number, number];
    xLabel?: string;
    yLabel?: string;
    /** Tick formatter. Defaults to `${v}%`. */
    formatTick?: (v: number) => string;
    /**
     * Optional threshold for "danger zone" shading. The rectangle from
     * [xDomain[0], threshold.x] × [yDomain[0], threshold.y] is shaded red.
     */
    dangerThreshold?: { x: number; y: number };
    /** Optional label rendered inside the danger zone (e.g. "Needs attention"). */
    dangerLabel?: string;
    /**
     * Optional threshold for "healthy zone" shading. The rectangle from
     * [threshold.x, xDomain[1]] × [threshold.y, yDomain[1]] is shaded green.
     */
    healthyThreshold?: { x: number; y: number };
    /** Optional label rendered inside the healthy zone (e.g. "Healthy"). */
    healthyLabel?: string;
    /** Called when a point is clicked. */
    onPointClick?: (point: ScatterPoint<T>) => void;
    /** Optional renderer for tooltip body — receives the hovered point. */
    renderTooltip?: (point: ScatterPoint<T>) => React.ReactNode;
    /** Chart height in pixels. Defaults to 360. */
    height?: number;
};

const DEFAULT_HEIGHT = 360;
const DEFAULT_DOMAIN: [number, number] = [0, 100];
const POINT_RADIUS = 7;
const POINT_HOVER_RADIUS = 9;
const POINT_HALO_RADIUS = 13;
const POINT_HALO_HOVER_RADIUS = 16;
const POINT_HALO_OPACITY = 0.22;
const POINT_HALO_HOVER_OPACITY = 0.35;
const Y_AXIS_LABEL_TRANSFORM = 'rotate(-90)';

const ChartWrapper = styled.div<{ $height: number }>`
    position: relative;
    width: 100%;
    height: ${({ $height }) => $height}px;
`;

const TooltipBox = styled.div`
    position: absolute;
    pointer-events: none;
    background: ${(props) => props.theme.colors.bg};
    color: ${(props) => props.theme.colors.textSecondary};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 6px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    padding: 6px 8px;
    font-size: 12px;
    line-height: 1.4;
    transform: translate(-50%, calc(-100% - 12px));
    white-space: nowrap;
    z-index: 10;
`;

const PointLabel = styled.text`
    font-size: 11px;
    font-weight: 500;
    pointer-events: none;
    user-select: none;
`;

const AxisLabel = styled.text`
    font-size: 11px;
    font-weight: 500;
    text-anchor: middle;
    pointer-events: none;
`;

const ZoneLabel = styled.text`
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    pointer-events: none;
    user-select: none;
`;

export function ScatterPlot<T = unknown>({
    points,
    xDomain = DEFAULT_DOMAIN,
    yDomain = DEFAULT_DOMAIN,
    xLabel,
    yLabel,
    formatTick = (v) => `${v}%`,
    dangerThreshold,
    dangerLabel,
    healthyThreshold,
    healthyLabel,
    onPointClick,
    renderTooltip,
    height = DEFAULT_HEIGHT,
}: Props<T>) {
    const theme = useTheme();
    const [hoveredId, setHoveredId] = useState<string | null>(null);

    const margin = { top: 16, right: 24, bottom: xLabel ? 56 : 36, left: yLabel ? 64 : 44 };

    return (
        <ChartWrapper $height={height}>
            <ParentSize>
                {({ width }) => {
                    if (!width) return null;
                    const innerWidth = Math.max(0, width - margin.left - margin.right);
                    const innerHeight = Math.max(0, height - margin.top - margin.bottom);

                    const xScale = scaleLinear<number>({
                        domain: xDomain,
                        range: [0, innerWidth],
                        clamp: true,
                    });
                    const yScale = scaleLinear<number>({
                        domain: yDomain,
                        range: [innerHeight, 0],
                        clamp: true,
                    });

                    const dangerWidth = dangerThreshold ? xScale(dangerThreshold.x) - xScale(xDomain[0]) : 0;
                    const dangerHeight = dangerThreshold ? yScale(yDomain[0]) - yScale(dangerThreshold.y) : 0;

                    const healthyWidth = healthyThreshold ? xScale(xDomain[1]) - xScale(healthyThreshold.x) : 0;
                    const healthyHeight = healthyThreshold ? yScale(healthyThreshold.y) - yScale(yDomain[1]) : 0;

                    const hoveredPoint = points.find((p) => p.id === hoveredId);

                    return (
                        <>
                            <svg width={width} height={height}>
                                <Group left={margin.left} top={margin.top}>
                                    {dangerThreshold && dangerWidth > 0 && dangerHeight > 0 && (
                                        <>
                                            <rect
                                                x={xScale(xDomain[0])}
                                                y={yScale(dangerThreshold.y)}
                                                width={dangerWidth}
                                                height={dangerHeight}
                                                fill={theme.colors.bgSurfaceError}
                                                opacity={0.4}
                                                rx={4}
                                            />
                                            {dangerLabel && (
                                                <ZoneLabel
                                                    x={xScale(xDomain[0]) + 8}
                                                    y={yScale(yDomain[0]) - 8}
                                                    fill={theme.colors.textError}
                                                >
                                                    {dangerLabel}
                                                </ZoneLabel>
                                            )}
                                        </>
                                    )}

                                    {healthyThreshold && healthyWidth > 0 && healthyHeight > 0 && (
                                        <>
                                            <rect
                                                x={xScale(healthyThreshold.x)}
                                                y={yScale(yDomain[1])}
                                                width={healthyWidth}
                                                height={healthyHeight}
                                                fill={theme.colors.bgSurfaceSuccess}
                                                opacity={0.35}
                                                rx={4}
                                            />
                                            {healthyLabel && (
                                                <ZoneLabel
                                                    x={xScale(xDomain[1]) - 8}
                                                    y={yScale(yDomain[1]) + 16}
                                                    textAnchor="end"
                                                    fill={theme.colors.textSuccess}
                                                >
                                                    {healthyLabel}
                                                </ZoneLabel>
                                            )}
                                        </>
                                    )}

                                    <GridRows
                                        scale={yScale}
                                        width={innerWidth}
                                        numTicks={5}
                                        stroke={theme.colors.border}
                                        strokeOpacity={0.6}
                                    />
                                    <GridColumns
                                        scale={xScale}
                                        height={innerHeight}
                                        numTicks={5}
                                        stroke={theme.colors.border}
                                        strokeOpacity={0.6}
                                    />

                                    <AxisLeft
                                        scale={yScale}
                                        numTicks={5}
                                        tickFormat={(v) => formatTick(v as number)}
                                        stroke={theme.colors.border}
                                        tickStroke={theme.colors.border}
                                        tickLabelProps={() => ({
                                            fill: theme.colors.textSecondary,
                                            fontSize: 11,
                                            textAnchor: 'end',
                                            dx: '-0.25em',
                                            dy: '0.33em',
                                        })}
                                    />
                                    <AxisBottom
                                        scale={xScale}
                                        top={innerHeight}
                                        numTicks={5}
                                        tickFormat={(v) => formatTick(v as number)}
                                        stroke={theme.colors.border}
                                        tickStroke={theme.colors.border}
                                        tickLabelProps={() => ({
                                            fill: theme.colors.textSecondary,
                                            fontSize: 11,
                                            textAnchor: 'middle',
                                        })}
                                    />

                                    {yLabel && (
                                        <AxisLabel
                                            x={-innerHeight / 2}
                                            y={-margin.left + 16}
                                            transform={Y_AXIS_LABEL_TRANSFORM}
                                            fill={theme.colors.textSecondary}
                                        >
                                            {yLabel}
                                        </AxisLabel>
                                    )}
                                    {xLabel && (
                                        <AxisLabel
                                            x={innerWidth / 2}
                                            y={innerHeight + margin.bottom - 8}
                                            fill={theme.colors.textSecondary}
                                        >
                                            {xLabel}
                                        </AxisLabel>
                                    )}

                                    {points.map((point) => {
                                        const cx = xScale(point.x);
                                        const cy = yScale(point.y);
                                        const isHovered = point.id === hoveredId;
                                        const fill = point.color ?? theme.colors.iconBrand;
                                        // Estimate label width (~7px per char @ fontSize 11) so we can flip
                                        // labels to the left when a dot is too close to the right edge.
                                        const estLabelWidth = (point.label?.length ?? 0) * 7 + 8;
                                        const flipLabelLeft = cx + POINT_HALO_RADIUS + 4 + estLabelWidth > innerWidth;
                                        const labelX = flipLabelLeft
                                            ? cx - POINT_HALO_RADIUS - 4
                                            : cx + POINT_HALO_RADIUS + 4;
                                        return (
                                            <g
                                                key={point.id}
                                                onMouseEnter={() => setHoveredId(point.id)}
                                                onMouseLeave={() => setHoveredId(null)}
                                                onClick={() => onPointClick?.(point)}
                                                style={{ cursor: onPointClick ? 'pointer' : 'default' }}
                                            >
                                                <circle
                                                    cx={cx}
                                                    cy={cy}
                                                    r={isHovered ? POINT_HALO_HOVER_RADIUS : POINT_HALO_RADIUS}
                                                    fill={fill}
                                                    fillOpacity={
                                                        isHovered ? POINT_HALO_HOVER_OPACITY : POINT_HALO_OPACITY
                                                    }
                                                />
                                                <circle
                                                    cx={cx}
                                                    cy={cy}
                                                    r={isHovered ? POINT_HOVER_RADIUS : POINT_RADIUS}
                                                    fill={fill}
                                                    stroke={theme.colors.bg}
                                                    strokeWidth={2}
                                                />
                                                {point.label && (
                                                    <PointLabel
                                                        x={labelX}
                                                        y={cy + 4}
                                                        textAnchor={flipLabelLeft ? 'end' : 'start'}
                                                        fill={theme.colors.text}
                                                    >
                                                        {point.label}
                                                    </PointLabel>
                                                )}
                                            </g>
                                        );
                                    })}
                                </Group>
                            </svg>

                            {hoveredPoint && renderTooltip && (
                                <TooltipBox
                                    style={{
                                        left: margin.left + xScale(hoveredPoint.x),
                                        top: margin.top + yScale(hoveredPoint.y),
                                    }}
                                >
                                    {renderTooltip(hoveredPoint)}
                                </TooltipBox>
                            )}
                        </>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
}
