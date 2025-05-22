import dayjs from 'dayjs';
import moment from 'moment';
import { useEffect, useMemo, useState } from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import {
    CustomOperationType,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import {
    convertAggregationsToOperationsData,
    getCustomOperationsFromAggregations,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { addMonthOverMonthValue } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import { useGetOperationsStatsBucketsLazyQuery } from '@src/graphql/dataset.generated';
import { OperationType, OperationsQueryResult, TimeRange } from '@src/types.generated';

type ResponseType = {
    buckets: CalendarData<OperationsData>[];
    summary: OperationsData | undefined;
    defaultOperationTypes: OperationType[];
    customOperationTypes: CustomOperationType[];
    loading: boolean;
};

const EMPTY_RESPONSE: ResponseType = {
    buckets: [],
    summary: undefined,
    defaultOperationTypes: [],
    customOperationTypes: [],
    loading: false,
};

function extractSummary(operationsStats: OperationsQueryResult | undefined | null) {
    return convertAggregationsToOperationsData(operationsStats?.aggregations);
}

function extractCustomOperationTypes(operationsStats: OperationsQueryResult | undefined | null) {
    return getCustomOperationsFromAggregations(operationsStats?.aggregations);
}

function extractDefaultOperationTypes(summary: OperationsData | undefined | null) {
    return Object.entries(summary?.operations || {})
        .map(([_, operation]) => operation)
        .filter((operation) => !operation.customType)
        .filter((operation) => operation.value > 0)
        .map((operation) => operation.type);
}

function extractBuckets(operationsStats: OperationsQueryResult | undefined | null, customTypes: CustomOperationType[]) {
    const convertedData = (operationsStats?.buckets ?? []).map((bucket) => {
        const value = convertAggregationsToOperationsData(bucket?.aggregations, customTypes);
        return {
            day: dayjs(bucket?.bucket).format(CALENDAR_DATE_FORMAT),
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
}

export default function useChangeHistoryData(urn: string | undefined, range: TimeRange | undefined): ResponseType {
    // Required for the loading state to track if the lazy query has been called
    const [queryCalled, setQueryCalled] = useState(false);
    const [getOperationsStatsBuckets, { data, loading }] = useGetOperationsStatsBucketsLazyQuery({
        onCompleted: () => setQueryCalled(true),
    });

    const {
        permissions: { canViewDatasetOperations },
    } = useStatsSectionsContext();

    useEffect(() => {
        if (urn && range && canViewDatasetOperations)
            getOperationsStatsBuckets({ variables: { urn, input: { range, timeZone: moment.tz.guess() } } });
    }, [urn, range, getOperationsStatsBuckets, canViewDatasetOperations]);

    return useMemo(() => {
        if (!canViewDatasetOperations) return EMPTY_RESPONSE;
        if (loading) return { ...EMPTY_RESPONSE, loading };

        const summary = extractSummary(data?.dataset?.operationsStats);
        const defaultOperationTypes = extractDefaultOperationTypes(summary);
        const customOperationTypes = extractCustomOperationTypes(data?.dataset?.operationsStats);
        const buckets = extractBuckets(data?.dataset?.operationsStats, customOperationTypes);

        return {
            buckets,
            summary,
            defaultOperationTypes,
            customOperationTypes,
            loading: queryCalled ? loading : true,
        };
    }, [canViewDatasetOperations, data, loading, queryCalled]);
}
