import { AxisScaleOutput } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { Margin } from '@visx/xychart';
import { RenderTooltipGlyphProps } from '@visx/xychart/lib/components/Tooltip';
import React from 'react';
import { AxisProps, GridProps } from '../BarChart/types';

export type LineChartProps<DatumType extends object> = {
    data: DatumType[];
    isEmpty?: boolean;

    xAccessor: (datum: DatumType) => string | number;
    yAccessor: (datum: DatumType) => number;
    xScale?: ScaleConfig<AxisScaleOutput, any, any>;
    yScale?: ScaleConfig<AxisScaleOutput, any, any>;
    maxYDomainForZeroData?: number;

    lineColor?: string;
    areaColor?: string;
    margin?: Margin;

    leftAxisProps?: AxisProps<DatumType>;
    bottomAxisProps?: AxisProps<DatumType>;
    gridProps?: GridProps<DatumType>;

    popoverRenderer?: (datum: DatumType) => React.ReactNode;
    renderGradients?: () => React.ReactNode;
    toolbarVerticalCrosshairStyle?: React.SVGProps<SVGLineElement>;
    renderTooltipGlyph?: (props: RenderTooltipGlyphProps<object>) => React.ReactNode | undefined;
};

export type TooltipGlyphProps = {
    x: number;
    y: number;
};
