import { useMemo } from 'react';

import { BaseDatum, YAccessor } from '@components/components/BarChart/types';

export default function useMinDataValue(data: BaseDatum[], yAccessor: YAccessor): number {
    return useMemo(() => Math.min(...data.map(yAccessor)) ?? 0, [data, yAccessor]);
}
