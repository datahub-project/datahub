import { AxisScaleOutput } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { Margin, TooltipDatum, GlyphProps as VisxGlyphProps } from '@visx/xychart';
import React from 'react';

import { AxisProps, BaseDatum, GridProps } from '@components/components/BarChart/types';

export type Datum = BaseDatum;

/** A single series within a multi-series line chart. */
export type LineChartSeries = {
    /** Stable, unique identifier for the series. Used as the visx dataKey. */
    dataKey: string;
    /** Optional human-readable name (e.g. shown in legends or tooltip rows). */
    name?: string;
    data: Datum[];
    lineColor?: string;
    areaColor?: string;
};

/** Context passed as the second arg of `popoverRenderer` for multi-series consumers. */
type LineChartPopoverContext = {
    /** All series rendered on the chart, in render order. */
    series: LineChartSeries[];
    /** Map from `dataKey` to the nearest datum for that series at the hovered x. */
    datumByKey: Record<string, TooltipDatum<Datum> | undefined>;
};

export type LineChartProps = {
    /**
     * Single-series data. Provide this OR `series` (multi-series).
     * If both are provided, `series` wins.
     */
    data?: Datum[];
    /**
     * Multi-series data. When provided and non-empty, replaces `data` and renders
     * one line per series sharing the same x/y scales.
     */
    series?: LineChartSeries[];
    isEmpty?: boolean;

    xScale?: ScaleConfig<AxisScaleOutput, any, any>;
    yScale?: ScaleConfig<AxisScaleOutput, any, any>;
    maxYDomainForZeroData?: number;
    shouldAdjustYZeroPoint?: boolean;
    yZeroPointThreshold?: number;

    /** Default line color when a series has none — also the single-series line color. */
    lineColor?: string;
    /** Default area fill when a series has none — also the single-series area color. */
    areaColor?: string;
    margin?: Partial<Margin>;

    leftAxisProps?: AxisProps;
    showLeftAxisLine?: boolean;
    bottomAxisProps?: AxisProps;
    showBottomAxisLine?: boolean;
    gridProps?: GridProps;

    /**
     * Render the tooltip popover. Receives the nearest datum across all series.
     * The optional second argument exposes per-series data so multi-series
     * consumers can render rows for each line at the hovered x.
     */
    popoverRenderer?: (datum: Datum, context?: LineChartPopoverContext) => React.ReactNode;
    renderGradients?: () => React.ReactNode;
    toolbarVerticalCrosshairStyle?: React.SVGProps<SVGLineElement>;
    renderTooltipGlyph?: (props: GlyphProps) => React.ReactElement | null;
    showGlyphOnSingleDataPoint?: boolean;
    renderGlyphOnSingleDataPoint?: React.FC<GlyphProps>;

    dataTestId?: string;
};

export type GlyphProps = VisxGlyphProps<Datum>;
