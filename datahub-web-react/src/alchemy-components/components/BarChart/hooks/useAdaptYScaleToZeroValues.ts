import { AxisScaleOutput } from '@visx/axis';
import { ScaleConfig } from '@visx/scale';
import { useMemo } from 'react';

export const DEFAULT_MAX_DOMAIN_VALUE = 10;

export default function useAdaptYScaleToZeroValues(
    yScale: ScaleConfig<AxisScaleOutput, any, any> | undefined,
    maxDataValue: number,
    maxDomainValue: number | undefined,
): ScaleConfig<AxisScaleOutput, any, any> | undefined {
    return useMemo(() => {
        // yScale should be passed for adaptation otherwise return it as is
        if (!yScale) return yScale;

        // Data contains non zero values, no need to adapt
        if (maxDataValue > 0) return yScale;

        // Add domain with max value to show data with only zero values correctly
        const domain: [number, number] = [0, maxDomainValue ?? DEFAULT_MAX_DOMAIN_VALUE];
        return { domain, ...yScale };
    }, [maxDataValue, maxDomainValue, yScale]);
}
