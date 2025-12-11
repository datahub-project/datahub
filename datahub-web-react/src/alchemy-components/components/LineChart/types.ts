/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    shouldAdjustYZeroPoint?: boolean;
    yZeroPointThreshold?: number;

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

    dataTestId?: string;
};

export type GlyphProps = VisxGlyphProps<Datum>;

export type TooltipGlyphProps = {
    x: number;
    y: number;
};
