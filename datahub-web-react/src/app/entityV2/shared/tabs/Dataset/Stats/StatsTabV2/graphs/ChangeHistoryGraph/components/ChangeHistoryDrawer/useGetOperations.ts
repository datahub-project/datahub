import { useGetOperationsQuery } from '@src/graphql/dataset.generated';
import { FacetFilterInput } from '@src/types.generated';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { uniq } from 'lodash';
import { useMemo } from 'react';
import { AnyOperationType } from '../../types';
import { isPrefixedAsCustom, removePrefixFromOperationType } from '../../utils';
import { OPERATIONS_LIMIT } from './constants';

dayjs.extend(utc);

export default function useGetOperations(
    urn: string,
    day: string | undefined,
    selectedOperationTypes: AnyOperationType[],
    selectedActors: string[],
) {
    const [start, end] = useMemo(() => {
        if (!day) return [undefined, undefined];
        const utcDay = dayjs(day).utc(true);
        return [utcDay.startOf('day').toDate().getTime(), utcDay.endOf('day').toDate().getTime()];
    }, [day]);

    const customOperationTypes = selectedOperationTypes
        .filter((operationType) => isPrefixedAsCustom(operationType))
        .map((operationType) => removePrefixFromOperationType(operationType));

    const actorsFilter: FacetFilterInput | undefined = useMemo(() => {
        if (selectedActors.length > 0) {
            return {
                field: 'actor',
                values: selectedActors,
            };
        }
        return undefined;
    }, [selectedActors]);

    const predefinedOperationTypesFilters = useMemo(() => {
        const collectedFilters: FacetFilterInput[] = [];
        collectedFilters.push({
            field: 'operationType',
            values: selectedOperationTypes,
        });

        if (actorsFilter) collectedFilters.push(actorsFilter);

        return collectedFilters;
    }, [actorsFilter, selectedOperationTypes]);

    const customOperationTypesFilters = useMemo(() => {
        const collectedFilters: FacetFilterInput[] = [];
        collectedFilters.push({
            field: 'customOperationType',
            values: customOperationTypes,
        });

        if (actorsFilter) collectedFilters.push(actorsFilter);

        return collectedFilters;
    }, [customOperationTypes, actorsFilter]);

    const { data: predefinedOperationTypesData, loading: predefinedOperationTypesDataloading } = useGetOperationsQuery({
        variables: {
            urn,
            limit: OPERATIONS_LIMIT,
            startTime: start,
            endTime: end,
            filters: {
                and: predefinedOperationTypesFilters,
            },
        },
    });

    const { data: customOperationTypesData, loading: customOperationTypesDataloading } = useGetOperationsQuery({
        variables: {
            urn,
            limit: OPERATIONS_LIMIT,
            startTime: start,
            endTime: end,
            filters: {
                and: customOperationTypesFilters,
            },
        },
    });

    // FYI: There are no 'or' filter so we make two requests and merge their results for now
    const operations = useMemo(() => {
        const predefinedOperations = predefinedOperationTypesData?.dataset?.operations || [];
        const customOperations = customOperationTypesData?.dataset?.operations || [];
        const allOperations = [...predefinedOperations, ...customOperations]
            .sort((a, b) => b.timestampMillis - a.timestampMillis)
            .slice(0, OPERATIONS_LIMIT);
        return allOperations;
    }, [predefinedOperationTypesData?.dataset?.operations, customOperationTypesData?.dataset?.operations]);

    const actors = useMemo(
        () => uniq(operations.filter((operation) => operation.actor).map((operation) => operation.actor || '')),
        [operations],
    );

    const loading = predefinedOperationTypesDataloading || customOperationTypesDataloading;

    return { operations, actors, loading };
}
