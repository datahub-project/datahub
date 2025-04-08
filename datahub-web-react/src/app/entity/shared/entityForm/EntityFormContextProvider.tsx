import React, { useEffect, useState } from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import {
    getFormAssociation,
    isFormVerificationType,
} from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { EntityFormContext, FormView } from '@app/entity/shared/entityForm/EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '@app/entity/shared/entityForm/constants';
import { EntityAndType, GenericEntityProperties } from '@app/entity/shared/types';
import usePrevious from '@app/shared/usePrevious';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetDatasetQuery } from '@graphql/dataset.generated';
import { Entity } from '@types';

interface Props {
    children: React.ReactNode;
    formUrn: string;
}

export default function EntityFormContextProvider({ children, formUrn }: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityData, refetch: refetchEntityProfile, loading: profileLoading } = useEntityContext();
    const formAssociation = getFormAssociation(formUrn, entityData);
    const initialPromptId =
        formAssociation?.form?.info?.prompts?.filter((prompt) => !SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type))[0]
            ?.id || null;
    const isVerificationType = isFormVerificationType(entityData, formUrn);
    const [formView, setFormView] = useState<FormView>(FormView.BY_ENTITY);
    const [selectedEntity, setSelectedEntity] = useState<Entity>(entityData as Entity);
    const [selectedPromptId, setSelectedPromptId] = useState<string | null>(initialPromptId);
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const [shouldRefetchSearchResults, setShouldRefetchSearchResults] = useState(false);

    useEffect(() => {
        if (!selectedPromptId && formAssociation) {
            setSelectedPromptId(initialPromptId);
        }
    }, [selectedPromptId, formAssociation, initialPromptId]);

    const previousFormUrn = usePrevious(formUrn);
    useEffect(() => {
        if (formUrn && previousFormUrn !== formUrn) {
            setFormView(FormView.BY_ENTITY);
            setSelectedPromptId(initialPromptId);
        }
    }, [formUrn, previousFormUrn, initialPromptId]);

    const query = entityRegistry.getEntityQuery(selectedEntity.type);
    const entityQuery = query || useGetDatasetQuery;
    const {
        data: fetchedData,
        refetch,
        loading,
    } = entityQuery({
        variables: { urn: selectedEntity.urn },
    });

    const isOnEntityProfilePage = selectedEntity.urn === entityData?.urn;
    const selectedEntityData = isOnEntityProfilePage ? entityData : (fetchedData?.dataset as GenericEntityProperties);

    return (
        <EntityFormContext.Provider
            value={{
                formUrn,
                isInFormContext: true,
                entityData: selectedEntityData as GenericEntityProperties,
                loading: isOnEntityProfilePage ? profileLoading : loading,
                refetch: isOnEntityProfilePage ? refetchEntityProfile : refetch,
                selectedEntity,
                setSelectedEntity,
                formView,
                setFormView,
                selectedPromptId,
                setSelectedPromptId,
                selectedEntities,
                setSelectedEntities,
                shouldRefetchSearchResults,
                setShouldRefetchSearchResults,
                isVerificationType,
            }}
        >
            {children}
        </EntityFormContext.Provider>
    );
}
