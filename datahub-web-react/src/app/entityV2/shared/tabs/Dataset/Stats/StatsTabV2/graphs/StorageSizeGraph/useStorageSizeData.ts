import { extractChartValuesFromTableProfiles } from '@src/app/entityV2/shared/utils';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { useGetDataProfilesLazyQuery } from '@src/graphql/dataset.generated';
import { useEffect, useMemo, useState } from 'react';
import { Datum } from '@src/alchemy-components/components/LineChart/types';
import { LookbackWindow } from '../../../lookbackWindows';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { addMonthOverMonthValue, groupTimeData, TimeInterval } from '../utils';

export interface StorageSizeData extends Datum {
    mom: number | null;
}

interface Response {
    data: StorageSizeData[];
    loading: boolean;
}

export default function useStorageSizeData(
    urn: string | undefined,
    lookbackWindow: LookbackWindow | undefined,
): Response {
    const {
        permissions: { canViewDatasetProfile },
    } = useStatsSectionsContext();

    // Required for the loading state to track if the lazy query has been called
    const [queryCalled, setQueryCalled] = useState(false);
    const [getDataProfiles, { data: profilesData, loading = true }] = useGetDataProfilesLazyQuery({
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
        const storageSizeValues = extractChartValuesFromTableProfiles(profiles, 'sizeInBytes');
        return storageSizeValues;
    }, [profilesData?.dataset?.datasetProfiles]);

    const data = useMemo(() => {
        const reversedRawData = rawData.reverse();
        const groupedData = groupTimeData(
            reversedRawData,
            TimeInterval.DAY,
            (d) => d.timeMs,
            (d) => d.value,
            (values) => Math.max(...values),
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
