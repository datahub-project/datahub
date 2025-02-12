import { AxisScaleOutput } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { useMemo } from 'react';
import { BaseDatum } from '../types';

export const DEFAULT_MAX_DOMAIN_VALUE = 10;

export default function useAdaptYScaleToZeroValues(
    data: BaseDatum[],
    yScale: ScaleConfig<AxisScaleOutput, any, any> | undefined,
    maxDomainValue: number | undefined,
): ScaleConfig<AxisScaleOutput, any, any> | undefined {
    return useMemo(() => {
        // yScale should be passed for adaptation otherwise return it as is
        if (!yScale) return yScale;

        const hasNonZeroValues = data.filter((datum) => datum.y !== 0).length > 0;
        // Data contains non zero values, no need to adapt
        if (hasNonZeroValues) return yScale;

        // Add domain with max value to show data with only zero values correctly
        const domain: [number, number] = [0, maxDomainValue ?? DEFAULT_MAX_DOMAIN_VALUE];
        return { domain, ...yScale };
    }, [data, maxDomainValue, yScale]);
}
