/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useMemo } from 'react';

import { BaseDatum, XAccessor, YAccessor } from '@components/components/BarChart/types';

export const DEFAULT_MIN_VALUE = 0.1;

export default function usePrepareAccessors(
    data: BaseDatum[],
    horizontal: boolean,
    xAccessor: XAccessor,
    yAccessor: YAccessor,
    minimalValue?: number,
) {
    const setMinimalValueForZeroData = useCallback(
        (accessor: XAccessor | YAccessor) => {
            const hasNonZeroValues = data.filter((datum) => accessor(datum) !== 0).length > 0;
            if (hasNonZeroValues) return accessor;
            return (value: BaseDatum) => Math.max(accessor(value), minimalValue ?? DEFAULT_MIN_VALUE);
        },
        [data, minimalValue],
    );

    const accessors = useMemo(
        () => ({
            xAccessor: horizontal ? setMinimalValueForZeroData(xAccessor) : xAccessor,
            yAccessor: horizontal ? yAccessor : setMinimalValueForZeroData(yAccessor),
        }),
        [yAccessor, xAccessor, horizontal, setMinimalValueForZeroData],
    );

    return accessors;
}
