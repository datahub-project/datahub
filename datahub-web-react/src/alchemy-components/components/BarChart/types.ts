import { AxisScaleOutput, TickFormatter, TickLabelProps } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { Margin } from '@visx/xychart';

export type BarChartProps<DatumType extends object> = {
    data: DatumType[];
    xAccessor: (datum: DatumType) => string | number;
    yAccessor: (datum: DatumType) => number;
    renderTooltipContent?: (datum: DatumType) => React.ReactNode;
    margin?: Margin;
    leftAxisTickFormat?: TickFormatter<DatumType>;
    leftAxisTickLabelProps?: TickLabelProps<DatumType>;
    bottomAxisTickFormat?: TickFormatter<DatumType>;
    bottomAxisTickLabelProps?: TickLabelProps<DatumType>;
    barColor?: string;
    barSelectedColor?: string;
    gridColor?: string;
    renderGradients?: () => React.ReactNode;
    isEmpty?: boolean;
    xScale?: ScaleConfig<AxisScaleOutput, any, any>;
    yScale?: ScaleConfig<AxisScaleOutput, any, any>;
};
