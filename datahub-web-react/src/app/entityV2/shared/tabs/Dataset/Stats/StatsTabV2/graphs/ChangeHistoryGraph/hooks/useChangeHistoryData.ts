import { useGetOperationsStatsBucketsLazyQuery } from '@src/graphql/dataset.generated';
import { TimeRange } from '@src/types.generated';
import dayjs from 'dayjs';
import { useEffect, useMemo } from 'react';
import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { addMonthOverMonthValue } from '../../utils';

export type ValueType = {
    total: number;
    inserts: number;
    updates: number;
    deletes: number;
    mom: number | null;
};

export type DatumType = {
    day: string;
    value: ValueType;
};

type Response = {
    data: DatumType[];
    loading: boolean;
};

export default function useChangeHistoryData(urn: string | undefined, range: TimeRange | undefined): Response {
    const [getOperationsStatsBuckets, { data, loading }] = useGetOperationsStatsBucketsLazyQuery();

    useEffect(() => {
        if (urn && range) getOperationsStatsBuckets({ variables: { urn, input: { range } } });
    }, [urn, range, getOperationsStatsBuckets]);

    const preparedData = useMemo(() => {
        if (loading) return [];

        const convertedData = (data?.dataset?.operationsStats?.buckets ?? []).map((bucket) => {
            const inserts = bucket?.aggregations?.totalInserts ?? 0;
            const updates = bucket?.aggregations?.totalUpdates ?? 0;
            const deletes = bucket?.aggregations?.totalDeletes ?? 0;
            const total = inserts + updates + deletes;

            return {
                day: dayjs(bucket?.bucket).utcOffset(0).utc(true).format(CALENDAR_DATE_FORMAT),
                value: {
                    total,
                    inserts,
                    updates,
                    deletes,
                },
            };
        });

        return addMonthOverMonthValue(
            convertedData,
            (d) => d.day,
            (d) => d.value.total,
        ).map((datum) => ({
            day: datum.day,
            value: { ...datum.value, mom: datum.mom },
        }));
    }, [data, loading]);

    return { data: preparedData as DatumType[], loading };
}
