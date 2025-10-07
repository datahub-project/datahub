import { useCallback, useMemo } from 'react';

import { BaseDatum, Scale, XAccessor, YAccessor } from '@components/components/BarChart/types';

export const DEFAULT_MAX_DOMAIN_VALUE = 10;

export interface Settings {
    horizontal?: boolean;
    maxDomainValueForZeroData: number | undefined;
}

export default function usePreparedScales(
    data: BaseDatum[],
    xScale: Scale | undefined,
    xAccessorOriginal: XAccessor,
    yScale: Scale | undefined,
    yAccessorOriginal: YAccessor,
    settings?: Settings,
) {
    const setDomainForZeroData = useCallback(
        (scale: Scale | undefined, accessor: XAccessor | YAccessor) => {
            if (!scale) return scale;
            const hasNonZeroValues = data.filter((datum) => accessor(datum) !== 0).length > 0;
            if (hasNonZeroValues) return scale;
            const domain: [number, number] = [0, settings?.maxDomainValueForZeroData ?? DEFAULT_MAX_DOMAIN_VALUE];
            return { domain, ...scale };
        },
        [data, settings],
    );

    const scales = useMemo(
        () => ({
            xScale: settings?.horizontal ? setDomainForZeroData(xScale, xAccessorOriginal) : xScale,
            yScale: settings?.horizontal ? yScale : setDomainForZeroData(yScale, yAccessorOriginal),
        }),
        [yScale, yAccessorOriginal, xScale, xAccessorOriginal, settings, setDomainForZeroData],
    );

    return scales;
}
