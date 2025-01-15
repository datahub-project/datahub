import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { useGetOperationsStatsBucketsLazyQuery } from '@src/graphql/dataset.generated';
import { TimeRange } from '@src/types.generated';
import dayjs from 'dayjs';
import { useEffect, useMemo } from 'react';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import { useStatsSectionsContext } from '../../../StatsSectionsContext';
import { addMonthOverMonthValue } from '../../utils';
import { CustomOperationType, OperationsData } from '../types';
import { convertAggregationsToValue } from '../utils';

type ResponseType = {
    data: CalendarData<OperationsData>[];
    loading: boolean;
};

export default function useChangeHistoryData(
    urn: string | undefined,
    range: TimeRange | undefined,
    defaultCustomOperationTypes?: CustomOperationType[],
): ResponseType {
    const [getOperationsStatsBuckets, { data, loading }] = useGetOperationsStatsBucketsLazyQuery();

    const {
        permissions: { canViewDatasetOperations },
    } = useStatsSectionsContext();

    useEffect(() => {
        if (urn && range && canViewDatasetOperations)
            getOperationsStatsBuckets({ variables: { urn, input: { range } } });
    }, [urn, range, getOperationsStatsBuckets, canViewDatasetOperations]);

    const preparedData = useMemo(() => {
        if (loading) return [];
        const convertedData = (data?.dataset?.operationsStats?.buckets ?? []).map((bucket) => {
            const value = convertAggregationsToValue(bucket?.aggregations, defaultCustomOperationTypes);
            return {
                day: dayjs(bucket?.bucket).utcOffset(0).utc(true).format(CALENDAR_DATE_FORMAT),
                value,
            };
        });

        // FYI: the data from backend come unsorted. Sort them by date
        const sortedData = [...convertedData].sort((a, b) => a.day.localeCompare(b.day));

        return addMonthOverMonthValue(
            sortedData,
            (d) => d.day,
            (d) => d.value?.summary?.totalOperations,
        ).map((datum) => ({
            day: datum.day,
            value: { ...datum.value, mom: datum.mom },
        }));
    }, [data, loading, defaultCustomOperationTypes]);

    if (!canViewDatasetOperations) {
        return {
            data: [],
            loading: false,
        };
    }

    return { data: preparedData as CalendarData<OperationsData>[], loading };
}
