import { extractChartValuesFromTableProfiles } from '@src/app/entityV2/shared/utils';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { useGetDataProfilesLazyQuery } from '@src/graphql/dataset.generated';
import { useEffect, useMemo } from 'react';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { addMonthOverMonthValue, groupTimeData, TimeInterval } from '../utils';

export default function useRowCountData(urn: string | undefined, lookbackWindow) {
    const [getDataProfiles, { data: profilesData, loading }] = useGetDataProfilesLazyQuery();

    const {
        permissions: { canViewDatasetProfile },
    } = useStatsSectionsContext();

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
            (values) => Math.max(...values),
        );

        return addMonthOverMonthValue(
            groupedData,
            (d) => d.time,
            (d) => d.value,
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
        loading,
    };
}
