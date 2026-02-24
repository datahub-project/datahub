import { QueryHookOptions, QueryResult } from '@apollo/client';

import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { GenericEntityProperties } from '@app/entity/shared/types';

import { GetFormsForEntityQuery } from '@graphql/form.generated';
import { EntityType, Exact } from '@types';

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
    getOverrideProperties: (T) => GenericEntityProperties;
    formsData?: GetFormsForEntityQuery;
}

export default function useGetDataForProfile<T>({
    urn,
    entityType,
    useEntityQuery,
    getOverrideProperties,
    formsData,
}: Props<T>) {
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
                data: {
                    ...formsData?.entity,
                    ...dataPossiblyCombinedWithSiblings[Object.keys(dataPossiblyCombinedWithSiblings)[0]],
                },
                entityType,
                getOverrideProperties,
                isHideSiblingMode,
            })) ||
        null;

    return { entityData, dataPossiblyCombinedWithSiblings, dataNotCombinedWithSiblings, loading, error, refetch };
}
