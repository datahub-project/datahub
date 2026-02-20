import { useEntityContext } from '@app/entity/shared/EntityContext';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetEntityQuery } from '@src/graphql/entity.generated';
import { Entity } from '@src/types.generated';

import { useGetFormsForEntityQuery } from '@graphql/form.generated';

interface Props {
    selectedEntity?: Entity;
}

export default function useEntityDataForForm({ selectedEntity }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const { entityData } = useEntityContext();

    const query = selectedEntity ? entityRegistry.getEntityQuery(selectedEntity.type) : null;
    const entityQuery = query || useGetEntityQuery;
    const {
        data: fetchedData,
        refetch: entityRefetch,
        loading: entityLoading,
    } = entityQuery({
        variables: { urn: selectedEntity?.urn || '' },
        skip: !selectedEntity,
        fetchPolicy: 'no-cache',
    });

    const { data: formsData, loading: formsLoading } = useGetFormsForEntityQuery({
        variables: { urn: selectedEntity?.urn || '' },
        fetchPolicy: 'no-cache',
        skip: !selectedEntity,
    });

    const isOnEntityProfilePage = !!selectedEntity && selectedEntity.urn === entityData?.urn;
    const selectedEntityGraphName = selectedEntity ? entityRegistry.getGraphNameFromType(selectedEntity.type) : '';
    const fetchedEntityData = fetchedData ? fetchedData[selectedEntityGraphName] : null;
    const fetchedGenericEntityData =
        selectedEntity && fetchedEntityData
            ? entityRegistry.getGenericEntityProperties(selectedEntity.type, fetchedEntityData)
            : null;
    const selectedEntityData = isOnEntityProfilePage ? entityData : fetchedGenericEntityData;

    return {
        selectedEntityData: !fetchedData || !formsData ? undefined : { ...formsData?.entity, ...selectedEntityData },
        isOnEntityProfilePage,
        entityRefetch,
        entityLoading: entityLoading || formsLoading,
    };
}
