import { TickFormatter, TickLabelProps } from '@visx/axis';
import { Margin } from '@visx/xychart';
import { RenderTooltipGlyphProps } from '@visx/xychart/lib/components/Tooltip';
import React from 'react';

export type LineChartProps<DatumType extends object> = {
    data: DatumType[];
    xAccessor: (datum: DatumType) => string | number;
    yAccessor: (datum: DatumType) => number;
    renderTooltipContent?: (datum: DatumType) => React.ReactNode;
    margin?: Margin;
    leftAxisTickFormat?: TickFormatter<DatumType>;
    leftAxisTickLabelProps?: TickLabelProps<DatumType>;
    bottomAxisTickFormat?: TickFormatter<DatumType>;
    bottomAxisTickLabelProps?: TickLabelProps<DatumType>;
    lineColor?: string;
    areaColor?: string;
    gridColor?: string;
    renderGradients?: () => React.ReactNode;
    toolbarVerticalCrosshairStyle?: React.SVGProps<SVGLineElement>;
    renderTooltipGlyph?: (props: RenderTooltipGlyphProps<object>) => React.ReactNode | undefined;
};
