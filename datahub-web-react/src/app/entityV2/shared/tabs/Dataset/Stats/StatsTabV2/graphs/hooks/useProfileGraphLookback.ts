import { useCallback, useEffect, useRef, useState } from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import { getInitialLookbackWindowType } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/getInitialLookbackWindowType';
import { useGetStatsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData';

export default function useProfileGraphLookback() {
    const { latestFullTableProfileTime, latestPartitionProfileTime } = useGetStatsData();
    const { statsEntityUrn } = useStatsSectionsContext();
    const userSelectedRef = useRef(false);
    const previousUrnRef = useRef(statsEntityUrn);

    const [rangeType, setRangeType] = useState<LookbackWindowType>(() =>
        getInitialLookbackWindowType([latestFullTableProfileTime, latestPartitionProfileTime]),
    );

    if (previousUrnRef.current !== statsEntityUrn) {
        previousUrnRef.current = statsEntityUrn;
        userSelectedRef.current = false;
    }

    useEffect(() => {
        if (userSelectedRef.current) return;
        setRangeType(getInitialLookbackWindowType([latestFullTableProfileTime, latestPartitionProfileTime]));
    }, [latestFullTableProfileTime, latestPartitionProfileTime, statsEntityUrn]);

    const selectRangeType = useCallback((value: string) => {
        if (!(value in GRAPH_LOOKBACK_WINDOWS)) return;
        userSelectedRef.current = true;
        setRangeType(value as LookbackWindowType);
    }, []);

    return {
        rangeType,
        lookbackWindow: GRAPH_LOOKBACK_WINDOWS[rangeType],
        selectRangeType,
    };
}
