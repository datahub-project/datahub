import { QueryHookOptions, QueryResult } from '@apollo/client';
import { getDataForEntityType } from './utils';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../siblingUtils';
import { GenericEntityProperties } from '../../types';
import { EntityType, Exact } from '../../../../../types.generated';

interface Props<T> {
    urn: string;
    count?: number;
    start?: number;
    entityType: EntityType;
    useEntityQuery: (
        baseOptions: QueryHookOptions<
            T,
            Exact<{
                urn: string;
                count?: number;
                start?: number;
            }>
        >,
    ) => QueryResult<
        T,
        Exact<{
            urn: string;
            count?: number;
            start?: number;
        }>
    >;
    getOverrideProperties: (T) => GenericEntityProperties;
}

export default function useGetDataForProfile<T>({
    urn,
    count,
    start,
    entityType,
    useEntityQuery,
    getOverrideProperties,
}: Props<T>) {
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const {
        loading,
        error,
        data: dataNotCombinedWithSiblings,
        refetch,
    } = useEntityQuery({
        variables: { urn, count, start },
        fetchPolicy: 'cache-first',
    });

    const dataPossiblyCombinedWithSiblings = isHideSiblingMode
        ? dataNotCombinedWithSiblings
        : combineEntityDataWithSiblings(dataNotCombinedWithSiblings);

    const entityData =
        (dataPossiblyCombinedWithSiblings &&
            Object.keys(dataPossiblyCombinedWithSiblings).length > 0 &&
            getDataForEntityType({
                data: dataPossiblyCombinedWithSiblings[Object.keys(dataPossiblyCombinedWithSiblings)[0]],
                entityType,
                getOverrideProperties,
                isHideSiblingMode,
            })) ||
        null;

    return { entityData, dataPossiblyCombinedWithSiblings, dataNotCombinedWithSiblings, loading, error, refetch };
}
