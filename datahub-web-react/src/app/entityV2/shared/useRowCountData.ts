import { useEffect, useMemo, useState } from 'react';

import {
    MAX_VALUE_AGGREGATION,
    TimeInterval,
    addMonthOverMonthValue,
    groupTimeData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { Datum } from '@src/alchemy-components/components/LineChart/types';
import { extractChartValuesFromTableProfiles } from '@src/app/entityV2/shared/utils';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { useGetDataProfilesLazyQuery } from '@src/graphql/dataset.generated';

export interface RowCountData extends Datum {
    mom?: number | null;
}

interface Response {
    data: RowCountData[];
    loading: boolean;
}

export default function useRowCountData(
    urn: string | undefined,
    lookbackWindow,
    canViewDatasetProfile: boolean,
): Response {
    // Required for the loading state to track if the lazy query has been called
    const [queryCalled, setQueryCalled] = useState(false);

    const [getDataProfiles, { data: profilesData, loading }] = useGetDataProfilesLazyQuery({
        onCompleted: () => setQueryCalled(true),
    });

    useEffect(() => {
        if (urn !== undefined && lookbackWindow !== undefined && canViewDatasetProfile) {
            getDataProfiles({
                variables: { urn, ...getFixedLookbackWindow(lookbackWindow.windowSize) },
            });
        }
    }, [urn, lookbackWindow, getDataProfiles, canViewDatasetProfile]);

    const rawData = useMemo(() => {
        const profiles = profilesData?.dataset?.datasetProfiles || [];
        const rowCountChartValues = extractChartValuesFromTableProfiles(profiles, 'rowCount');
        return rowCountChartValues;
    }, [profilesData?.dataset?.datasetProfiles]);

    const data = useMemo(() => {
        const reversedRawData = rawData.reverse();
        const groupedData = groupTimeData(
            reversedRawData,
            TimeInterval.DAY,
            (d) => d.timeMs,
            (d) => d.value,
            MAX_VALUE_AGGREGATION,
        );

        const convertedData = groupedData.map((datum) => ({ x: datum.time, y: datum.value }));

        return addMonthOverMonthValue(
            convertedData,
            (d) => d.x,
            (d) => d.y,
        );
    }, [rawData]);

    if (!canViewDatasetProfile) {
        return {
            data: [],
            loading: false,
        };
    }

    return {
        data,
        loading: queryCalled ? loading : true,
    };
}
