import { useMemo } from 'react';
import { YAccessor } from '../types';

export default function useMaxDataValue<T>(data: T[], yAccessor: YAccessor<T>): number {
    return useMemo(() => Math.max(...data.map(yAccessor)) ?? 0, [data, yAccessor]);
}
