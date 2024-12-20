import { useGetOperationsQuery } from '@src/graphql/dataset.generated';
import { FacetFilterInput, OperationType } from '@src/types.generated';
import dayjs from 'dayjs';
import { useMemo } from 'react';
import { uniq } from 'lodash';
import { OPERATIONS_LIMIT } from './constants';

export default function useGetOperations(
    urn: string,
    day: string | undefined,
    selectedOperationTypes: OperationType[],
    selectedActors: string[],
) {
    const [start, end] = useMemo(() => {
        if (!day) return [undefined, undefined];
        const utcDay = dayjs(day).utc(true);
        return [utcDay.startOf('day').toDate().getTime(), utcDay.endOf('day').toDate().getTime()];
    }, [day]);

    const filters = useMemo(() => {
        const collectedFilters: FacetFilterInput[] = [];
        collectedFilters.push({
            field: 'operationType',
            values: selectedOperationTypes,
        });

        if (selectedActors.length > 0) {
            collectedFilters.push({
                field: 'actor',
                values: selectedActors,
            });
        }

        return collectedFilters;
    }, [selectedOperationTypes, selectedActors]);

    const { data, loading } = useGetOperationsQuery({
        variables: {
            urn,
            limit: OPERATIONS_LIMIT,
            startTime: start,
            endTime: end,
            filters: {
                and: filters,
            },
        },
    });

    const operations = useMemo(() => data?.dataset?.operations || [], [data?.dataset?.operations]);

    const actors = useMemo(
        () => uniq(operations.filter((operation) => operation.actor).map((operation) => operation.actor || '')),
        [operations],
    );

    return { operations, actors, loading };
}
