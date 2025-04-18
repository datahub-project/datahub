import { useMemo } from 'react';
import { BaseDatum, YAccessor } from '../types';

export default function useMaxDataValue(data: BaseDatum[], yAccessor: YAccessor): number {
    return useMemo(() => Math.max(...data.map(yAccessor)) ?? 0, [data, yAccessor]);
}
