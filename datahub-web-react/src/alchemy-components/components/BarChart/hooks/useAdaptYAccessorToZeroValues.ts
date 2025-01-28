import { useMemo } from 'react';
import { YAccessor } from '../types';

export const DEFAULT_MIN_VALUE = 0.1;

export default function useAdaptYAccessorToZeroValue<T>(
    yAccessor: YAccessor<T>,
    maxDataValue: number,
    minimalValue: number | undefined,
): YAccessor<T> {
    return useMemo(() => {
        // Data contains non zero values, skip adaptation
        if (maxDataValue > 0) return yAccessor;

        // add minimal `y` value
        return (value) => Math.max(yAccessor(value), minimalValue ?? DEFAULT_MIN_VALUE);
    }, [yAccessor, maxDataValue, minimalValue]);
}
