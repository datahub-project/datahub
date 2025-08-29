import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { uniq } from 'lodash';
import { useMemo } from 'react';

import { OPERATIONS_LIMIT } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/constants';
import { AnyOperationType } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import {
    hasPrefix,
    removePrefix,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { useGetOperationsQuery } from '@src/graphql/dataset.generated';
import { FacetFilterInput, FilterOperator } from '@src/types.generated';

dayjs.extend(utc);

const TIMESTAMP_FIELD = 'lastUpdatedTimestamp';

export default function useGetOperations(
    urn: string,
    day: string | undefined | null,
    selectedOperationTypes: AnyOperationType[],
    selectedActors: string[] | undefined,
) {
    const [start, end] = useMemo(() => {
        if (!day) return [undefined, undefined];
        return [dayjs(day).startOf('day').toDate().getTime(), dayjs(day).endOf('day').toDate().getTime()];
    }, [day]);

    const customOperationTypes = selectedOperationTypes
        .filter((operationType) => hasPrefix(operationType))
        .map((operationType) => removePrefix(operationType));

    const actorsFilter: FacetFilterInput | undefined = useMemo(() => {
        if (selectedActors !== undefined) {
            return {
                field: 'actor',
                values: selectedActors,
            };
        }
        return undefined;
    }, [selectedActors]);

    const timeFilters = useMemo(() => {
        if (!start || !end) return [];
        return [
            {
                field: TIMESTAMP_FIELD,
                condition: FilterOperator.GreaterThanOrEqualTo,
                values: [start.toString()],
            },
            {
                field: TIMESTAMP_FIELD,
                condition: FilterOperator.LessThanOrEqualTo,
                values: [end.toString()],
            },
        ];
    }, [start, end]);

    const predefinedOperationTypesFilters = useMemo(() => {
        const collectedFilters: FacetFilterInput[] = [];
        collectedFilters.push(
            {
                field: 'operationType',
                values: selectedOperationTypes,
            },
            ...timeFilters,
        );

        if (actorsFilter) collectedFilters.push(actorsFilter);

        return collectedFilters;
    }, [actorsFilter, selectedOperationTypes, timeFilters]);

    const customOperationTypesFilters = useMemo(() => {
        const collectedFilters: FacetFilterInput[] = [];
        collectedFilters.push(
            {
                field: 'customOperationType',
                values: customOperationTypes,
            },
            ...timeFilters,
        );

        if (actorsFilter) collectedFilters.push(actorsFilter);

        return collectedFilters;
    }, [customOperationTypes, actorsFilter, timeFilters]);

    const { data: predefinedOperationTypesData, loading: predefinedOperationTypesDataloading } = useGetOperationsQuery({
        variables: {
            urn,
            limit: OPERATIONS_LIMIT,
            filters: {
                and: predefinedOperationTypesFilters,
            },
        },
    });

    const { data: customOperationTypesData, loading: customOperationTypesDataloading } = useGetOperationsQuery({
        variables: {
            urn,
            limit: OPERATIONS_LIMIT,
            filters: {
                and: customOperationTypesFilters,
            },
        },
    });

    // FYI: There are no 'or' filter so we make two requests and merge their results for now
    const operations = useMemo(() => {
        const predefinedOperations = predefinedOperationTypesData?.dataset?.operations || [];
        const customOperations = customOperationTypesData?.dataset?.operations || [];
        const allOperations = [...predefinedOperations, ...customOperations].sort(
            (a, b) => b.lastUpdatedTimestamp - a.lastUpdatedTimestamp,
        );
        return allOperations;
    }, [predefinedOperationTypesData?.dataset?.operations, customOperationTypesData?.dataset?.operations]);

    const actors = useMemo(
        () => uniq(operations.filter((operation) => operation.actor).map((operation) => operation.actor || '')),
        [operations],
    );

    const loading = predefinedOperationTypesDataloading || customOperationTypesDataloading;

    return { operations, actors, loading };
}
