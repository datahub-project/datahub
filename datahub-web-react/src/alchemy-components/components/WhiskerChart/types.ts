/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ChildRenderProps } from '@visx/stats/lib/types';
import { UseTooltipParams } from '@visx/tooltip/lib/hooks/useTooltip';
import React from 'react';

export interface TooltipRendererProps {
    x?: number | undefined;
    y?: number | undefined;
    minY?: number;
    maxY?: number;
    datum?: WhiskerTooltipDatum;
}

export interface ColorSchemeSettings {
    box: string;
    boxAlternative: string;
    medianLine: string;
    alternative: string;
}

export interface WhiskerDatum {
    key: string;
    min: number;
    firstQuartile: number;
    median: number;
    thirdQuartile: number;
    max: number;
    colorShemeSettings?: ColorSchemeSettings;
}

export enum WhiskerMetricType {
    Min = 'MIN',
    FirstQuartile = 'FIRST_QUARTILE',
    Median = 'MEDIAN',
    ThirdQuartile = 'THIRD_QUARTILE',
    Max = 'MAX',
}

export interface WhiskerTooltipDatum extends WhiskerDatum {
    type: WhiskerMetricType;
}

export interface WhiskerChartProps {
    data: WhiskerDatum[];
    boxSize?: number;
    gap?: number;
    axisLabel?: string;
    renderTooltip?: (props: TooltipRendererProps) => React.ReactNode;
    renderWhisker?: (props: WhiskerRenderProps) => React.ReactNode;
}

export type InternalWhiskerChartProps = WhiskerChartProps & {
    width: number;
    height: number;
    tooltip: UseTooltipParams<WhiskerTooltipDatum>;
};

export type WhiskerRenderProps = ChildRenderProps & {
    datum: WhiskerDatum;
    tooltip: UseTooltipParams<WhiskerTooltipDatum>;
};

export interface MetricPointProps {
    pointX: number;
    topOfWhiskerBar: number;
    heightOfWhiskerBar: number;
    overHandler: () => void;
    leaveHandler: () => void;
}
