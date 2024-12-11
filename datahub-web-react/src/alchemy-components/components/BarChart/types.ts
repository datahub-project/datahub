import { TickFormatter, TickLabelProps } from '@visx/axis';
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
};
