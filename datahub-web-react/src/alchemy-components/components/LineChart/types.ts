import { AxisScaleOutput } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { Margin, GlyphProps as VisxGlyphProps } from '@visx/xychart';
import React from 'react';

import { AxisProps, BaseDatum, GridProps } from '@components/components/BarChart/types';

export type Datum = BaseDatum;

export type LineChartProps = {
    data: Datum[];
    isEmpty?: boolean;

    xScale?: ScaleConfig<AxisScaleOutput, any, any>;
    yScale?: ScaleConfig<AxisScaleOutput, any, any>;
    maxYDomainForZeroData?: number;

    lineColor?: string;
    areaColor?: string;
    margin?: Partial<Margin>;

    leftAxisProps?: AxisProps;
    showLeftAxisLine?: boolean;
    bottomAxisProps?: AxisProps;
    showBottomAxisLine?: boolean;
    gridProps?: GridProps;

    popoverRenderer?: (datum: Datum) => React.ReactNode;
    renderGradients?: () => React.ReactNode;
    toolbarVerticalCrosshairStyle?: React.SVGProps<SVGLineElement>;
    renderTooltipGlyph?: (props: GlyphProps) => React.ReactElement | null;
    showGlyphOnSingleDataPoint?: boolean;
    renderGlyphOnSingleDataPoint?: React.FC<GlyphProps>;
};

export type GlyphProps = VisxGlyphProps<Datum>;

export type TooltipGlyphProps = {
    x: number;
    y: number;
};
