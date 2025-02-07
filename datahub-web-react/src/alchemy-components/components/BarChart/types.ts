import { AxisScaleOutput } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { Margin } from '@visx/xychart';
import { AxisProps as VisxAxisProps } from '@visx/xychart/lib/components/axis/Axis';
import { GridProps as VisxGridProps } from '@visx/xychart/lib/components/grid/Grid';

export type AxisProps<DatumType extends object> = Omit<VisxAxisProps, 'orientation' | 'numTicks'> & {
    computeNumTicks?: (width: number, height: number, margin: Margin, data: DatumType[]) => number | undefined;
};

export type GridProps<DatumType extends object> = Omit<VisxGridProps, 'numTicks'> & {
    computeNumTicks?: (width: number, height: number, margin: Margin, data: DatumType[]) => number | undefined;
};

export type YAccessor<T> = (datum: T) => number;

export type BarChartProps<DatumType extends object> = {
    data: DatumType[];
    isEmpty?: boolean;

    xAccessor: (datum: DatumType) => string | number;
    yAccessor: YAccessor<DatumType>;
    xScale?: ScaleConfig<AxisScaleOutput, any, any>;
    yScale?: ScaleConfig<AxisScaleOutput, any, any>;
    maxYDomainForZeroData?: number;
    minYForZeroData?: number;

    barColor?: string;
    barSelectedColor?: string;
    margin?: Margin;

    leftAxisProps?: AxisProps<DatumType>;
    bottomAxisProps?: AxisProps<DatumType>;
    gridProps?: GridProps<DatumType>;

    popoverRenderer?: (datum: DatumType) => React.ReactNode;
    renderGradients?: () => React.ReactNode;
};
