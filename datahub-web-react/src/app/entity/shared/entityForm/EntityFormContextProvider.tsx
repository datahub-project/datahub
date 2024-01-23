import React, { useEffect, useState } from 'react';
import { EntityFormContext, FormView } from './EntityFormContext';
import { useEntityContext } from '../EntityContext';
import { Entity } from '../../../../types.generated';
import { useGetDatasetQuery } from '../../../../graphql/dataset.generated';
import { EntityAndType, GenericEntityProperties } from '../types';
import { getFormAssociation, isFormVerificationType } from '../containers/profile/sidebar/FormInfo/utils';
import usePrevious from '../../../shared/usePrevious';
import { useUserContext } from '../../../context/useUserContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from './constants';
import { useSearchForEntitiesByFormCountQuery } from '../../../../graphql/form.generated';
import useFormFilter from './useFormFilter';

interface Props {
    children: React.ReactNode;
    formUrn: string;
}

export default function EntityFormContextProvider({ children, formUrn }: Props) {
    const { user } = useUserContext();
    const { entityData, refetch: refetchEntityProfile, loading: profileLoading } = useEntityContext();
    const formAssociation = getFormAssociation(formUrn, entityData);
    const initialPromptId =
        formAssociation?.form.info.prompts.filter((prompt) => !SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type))[0]
            ?.id || null;
    const isVerificationType = isFormVerificationType(entityData, formUrn);
    const [formView, setFormView] = useState<FormView>(FormView.BY_ENTITY);
    const [selectedEntity, setSelectedEntity] = useState<Entity>(entityData as Entity);
    const [selectedPromptId, setSelectedPromptId] = useState<string | null>(initialPromptId);
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const [shouldRefetchSearchResults, setShouldRefetchSearchResults] = useState(false);
    const { formFilter, formResponsesFilters, setFormResponsesFilters } = useFormFilter({
        formUrn,
        isVerificationType,
        formView,
        selectedPromptId,
    });

    const { data, refetch: refetchNumReadyForVerification } = useSearchForEntitiesByFormCountQuery({
        variables: {
            input: {
                formFilter: { formUrn, assignedActor: user?.urn, isFormVerified: false, isFormComplete: true },
                count: 0,
            },
        },
        skip: !isVerificationType,
    });

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

    const {
        data: fetchedData,
        refetch,
        loading,
    } = useGetDatasetQuery({
        variables: { urn: selectedEntity.urn },
    });

    const isOnEntityProfilePage = selectedEntity.urn === entityData?.urn;
    const selectedEntityData = isOnEntityProfilePage ? entityData : (fetchedData?.dataset as GenericEntityProperties);
    const displayBulkPromptStyles = formView === FormView.BY_QUESTION;

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
                displayBulkPromptStyles,
                formFilter,
                selectedEntities,
                setSelectedEntities,
                formResponsesFilters,
                setFormResponsesFilters,
                shouldRefetchSearchResults,
                setShouldRefetchSearchResults,
                numReadyForVerification: data?.searchForEntitiesByForm?.total || 0,
                refetchNumReadyForVerification,
                isVerificationType,
            }}
        >
            {children}
        </EntityFormContext.Provider>
    );
}
