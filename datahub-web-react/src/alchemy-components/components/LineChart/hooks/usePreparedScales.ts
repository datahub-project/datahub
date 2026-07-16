import { useMemo } from 'react';

import useBasicallyPreparedScales, {
    Settings as BasicSettings,
} from '@components/components/BarChart/hooks/usePreparedScales';
import { BaseDatum, Scale, XAccessor, YAccessor } from '@components/components/BarChart/types';

const DEFAULT_THRESHOLD_VALUE_TO_ADJUST_ZERO_POINT = 0.1;

// Horizontal line charts are not supported
interface Settings extends Omit<BasicSettings, 'horizontal'> {
    shouldAdjustYZeroPoint?: boolean;
    yZeroPointThreshold?: number;
}

export default function usePreparedLineChartScales(
    data: BaseDatum[],
    xScale: Scale | undefined,
    xAccessor: XAccessor,
    yScale: Scale | undefined,
    yAccessor: YAccessor,
    settings: Settings,
) {
    const { shouldAdjustYZeroPoint, yZeroPointThreshold, ...baseSettings } = settings;

    const basicallyPreparedScales = useBasicallyPreparedScales(
        data,
        xScale,
        xAccessor,
        yScale,
        yAccessor,
        baseSettings,
    );

    const scales = useMemo(() => {
        const yValues = data.map((datum) => yAccessor(datum));
        const minYValue = Math.min(...yValues);
        const maxYValue = Math.max(...yValues);

        const adjustZeroPoint = (scale: Scale | undefined) => {
            if (!scale) return scale;
            if (!shouldAdjustYZeroPoint) return scale;

            const hasNegativeValues = yValues.some((value) => value < 0);
            if (hasNegativeValues) return scale; // only positive values supported
            // Some scaling types don't support `zero` param
            if (['log', 'time', 'utc'].includes(scale.type)) return scale;

            // Values have the same value
            if (minYValue === maxYValue) return scale;

            const diffPercent = (maxYValue - minYValue) / maxYValue;
            if (diffPercent > (yZeroPointThreshold ?? DEFAULT_THRESHOLD_VALUE_TO_ADJUST_ZERO_POINT)) return scale;

            // Do not add zero to the scaling domain if we want to start line from value above zero
            return { ...scale, zero: false };
        };

        const adjustedYScale = adjustZeroPoint(basicallyPreparedScales.yScale);

        return {
            ...basicallyPreparedScales,
            yScale: adjustedYScale,
        };
    }, [data, basicallyPreparedScales, yAccessor, shouldAdjustYZeroPoint, yZeroPointThreshold]);

    return scales;
}
