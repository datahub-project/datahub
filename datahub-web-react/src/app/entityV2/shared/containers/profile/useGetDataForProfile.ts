import { useAppConfig } from '@src/app/useAppConfig';
import { QueryHookOptions, QueryResult } from '@apollo/client';
import { getDataForEntityType } from './utils';
import { useIsSeparateSiblingsMode } from '../../useIsSeparateSiblingsMode';
import { GenericEntityProperties } from '../../../../entity/shared/types';
import { EntityType, Exact } from '../../../../../types.generated';
import { combineEntityDataWithSiblings } from '../../../../entity/shared/siblingUtils';

interface Props<T> {
    urn: string;
    entityType: EntityType;
    useEntityQuery: (
        baseOptions: QueryHookOptions<
            T,
            Exact<{
                urn: string;
            }>
        >,
    ) => QueryResult<
        T,
        Exact<{
            urn: string;
        }>
    >;
    getOverrideProperties?: (T) => GenericEntityProperties;
}

export default function useGetDataForProfile<T>({ urn, entityType, useEntityQuery, getOverrideProperties }: Props<T>) {
    const flags = useAppConfig().config.featureFlags;
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const {
        loading,
        error,
        data: dataNotCombinedWithSiblings,
        refetch,
    } = useEntityQuery({
        variables: { urn },
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
                flags,
            })) ||
        null;

    return { entityData, dataPossiblyCombinedWithSiblings, dataNotCombinedWithSiblings, loading, error, refetch };
}
