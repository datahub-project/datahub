import { TickFormatter, TickLabelProps } from '@visx/axis';
import { Margin } from '@visx/xychart';

export type BarChartProps = {
    data: any;
    xAccessor: (datum: any) => any;
    yAccessor: (datum: any) => any;
    renderTooltipContent?: (datum: any) => React.ReactNode;
    margin?: Margin;
    leftAxisTickFormat?: TickFormatter<any>;
    leftAxisTickLabelProps?: TickLabelProps<any>;
    bottomAxisTickFormat?: TickFormatter<any>;
    bottomAxisTickLabelProps?: TickLabelProps<any>;
    barColor?: string;
    barSelectedColor?: string;
    gridColor?: string;
    renderGradients?: () => React.ReactNode;
};
