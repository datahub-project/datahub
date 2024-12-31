import { extractChartValuesFromTableProfiles } from '@src/app/entityV2/shared/utils';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { useGetDataProfilesLazyQuery } from '@src/graphql/dataset.generated';
import { useEffect, useMemo } from 'react';
import { addMonthOverMonthValue, groupTimeData } from '../utils';
import { LookbackWindow } from '../../../lookbackWindows';

export default function useStorageSizeData(urn: string | undefined, lookbackWindow: LookbackWindow | undefined) {
    const [getDataProfiles, { data: profilesData, loading }] = useGetDataProfilesLazyQuery();

    useEffect(() => {
        if (urn !== undefined && lookbackWindow !== undefined) {
            getDataProfiles({
                variables: { urn, ...getFixedLookbackWindow(lookbackWindow.windowSize) },
            });
        }
    }, [urn, lookbackWindow, getDataProfiles]);

    const rawData = useMemo(() => {
        const profiles = profilesData?.dataset?.datasetProfiles || [];
        const storageSizeValues = extractChartValuesFromTableProfiles(profiles, 'sizeInBytes');
        return storageSizeValues;
    }, [profilesData?.dataset?.datasetProfiles]);

    const data = useMemo(() => {
        const reversedRawData = rawData.reverse();
        const groupedData = groupTimeData(
            reversedRawData,
            'day',
            (d) => d.timeMs,
            (d) => d.value,
            (values) => Math.max(...values),
        );

        return addMonthOverMonthValue(
            groupedData,
            (d) => d.time,
            (d) => d.value,
        );
    }, [rawData]);

    return {
        data,
        loading,
    };
}
