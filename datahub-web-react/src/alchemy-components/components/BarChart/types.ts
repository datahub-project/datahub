/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AxisScaleOutput, TickRendererProps } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { Margin } from '@visx/xychart';
import { AxisProps as VisxAxisProps } from '@visx/xychart/lib/components/axis/Axis';
import { GridProps as VisxGridProps } from '@visx/xychart/lib/components/grid/Grid';

export enum ColorScheme {
    Violet = 'VIOLET',
    Blue = 'BLUE',
    Pink = 'PINK',
    Orange = 'ORANGE',
    Green = 'GREEN',
}

export interface BaseDatum {
    x: number;
    y: number;
}

export type Datum = BaseDatum & {
    colorScheme?: ColorScheme;
};

export type AxisProps = Omit<VisxAxisProps, 'orientation' | 'numTicks'> & {
    computeNumTicks?: (width: number, height: number, margin: Margin, data: BaseDatum[]) => number | undefined;
};

export type GridProps = Omit<VisxGridProps, 'numTicks'> & {
    computeNumTicks?: (width: number, height: number, margin: Margin, data: BaseDatum[]) => number | undefined;
};

export type ValueAccessor = (datum: BaseDatum) => number;
export type YAccessor = ValueAccessor;
export type XAccessor = ValueAccessor;

export type ColorAccessor = (datum: Datum, index: number) => string;

export type Scale = ScaleConfig<AxisScaleOutput, any, any>;

export type BarChartProps = {
    data: Datum[];
    isEmpty?: boolean;
    horizontal?: boolean;

    xScale?: Scale;
    yScale?: Scale;
    maxYDomainForZeroData?: number;
    minYForZeroData?: number;

    margin?: Partial<Margin>;

    leftAxisProps?: AxisProps;
    showLeftAxisLine?: boolean;
    maxLengthOfLeftAxisLabel?: number;
    bottomAxisProps?: AxisProps;
    gridProps?: GridProps;

    popoverRenderer?: (datum: Datum) => React.ReactNode;

    dataTestId?: string;
};

export type TruncatableTickProps = TickRendererProps & {
    limit?: number;
};

export type ColorSchemeParams = {
    mainColor: string;
    alternativeColor: string;
};
