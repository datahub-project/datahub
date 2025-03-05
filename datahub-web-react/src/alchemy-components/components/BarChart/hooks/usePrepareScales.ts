import { useCallback, useMemo } from 'react';
import { BaseDatum, Scale, XAccessor, YAccessor } from '../types';

export const DEFAULT_MAX_DOMAIN_VALUE = 10;

export default function usePrepareScales(
    data: BaseDatum[],
    horizontal: boolean,
    xScale: Scale | undefined,
    xAccessorOriginal: XAccessor,
    yScale: Scale | undefined,
    yAccessorOriginal: YAccessor,
    maxDomainValue: number | undefined,
) {
    const setDomainForZeroData = useCallback(
        (scale: Scale | undefined, accessor: XAccessor | YAccessor) => {
            if (!scale) return scale;
            const hasNonZeroValues = data.filter((datum) => accessor(datum) !== 0).length > 0;
            if (hasNonZeroValues) return scale;
            const domain: [number, number] = [0, maxDomainValue ?? DEFAULT_MAX_DOMAIN_VALUE];
            return { domain, ...scale };
        },
        [data, maxDomainValue],
    );

    const scales = useMemo(
        () => ({
            xScale: horizontal ? setDomainForZeroData(xScale, xAccessorOriginal) : xScale,
            yScale: horizontal ? yScale : setDomainForZeroData(yScale, yAccessorOriginal),
        }),
        [yScale, yAccessorOriginal, xScale, xAccessorOriginal, horizontal, setDomainForZeroData],
    );

    return scales;
}
