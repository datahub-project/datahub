import dayjs from 'dayjs';
import { useCallback } from 'react';

import { AxisProps } from '@src/alchemy-components/components/BarChart/types';
import { LookbackWindowType } from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';

export default function useGetBottomAxisPropsByLookbackWindowType(
    windowType: LookbackWindowType | string | null,
): AxisProps {
    const tickFormat = useCallback(
        (timestampMs: number) => {
            const timestamp = dayjs(timestampMs);
            switch (windowType) {
                case LookbackWindowType.Week:
                    return timestamp.format('D MMM ‘YY');
                case LookbackWindowType.Month:
                    return timestamp.format('D MMM ‘YY');
                default:
                    return timestamp.format('MMM ‘YY');
            }
        },
        [windowType],
    );

    return { tickFormat };
}
