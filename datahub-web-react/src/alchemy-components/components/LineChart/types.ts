import { AxisScaleOutput } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { GlyphProps, Margin } from '@visx/xychart';
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
    showLeftAxisLine?: boolean;
    bottomAxisProps?: AxisProps<DatumType>;
    showBottomAxisLine?: boolean;
    gridProps?: GridProps<DatumType>;

    popoverRenderer?: (datum: DatumType) => React.ReactNode;
    renderGradients?: () => React.ReactNode;
    toolbarVerticalCrosshairStyle?: React.SVGProps<SVGLineElement>;
    renderTooltipGlyph?: (props: GlyphPropsWithRef<object>) => React.ReactElement | null;
    showGlyphOnSingleDataPoint?: boolean;
    renderGlyphOnSingleDataPoint?: React.FC<GlyphProps<DatumType>>;
};

export type GlyphPropsWithRef<T extends object> = GlyphProps<T> & {
    ref?: React.RefObject<SVGGElement>;
};

export type TooltipGlyphProps = {
    x: number;
    y: number;
};
