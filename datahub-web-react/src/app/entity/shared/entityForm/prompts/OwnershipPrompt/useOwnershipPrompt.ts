import { isEqual } from 'lodash';
import { useCallback, useEffect, useMemo, useState } from 'react';

import { useEntityData, useMutationUrn } from '@app/entity/shared/EntityContext';
import { getPromptAssociation } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '@app/entity/shared/entityForm/constants';
import {
    getDefaultOwnerEntities,
    getDefaultOwnershipTypeUrn,
} from '@app/entity/shared/entityForm/prompts/OwnershipPrompt/utils';
import { useGetEntityWithSchema } from '@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema';

import { useBatchRemoveOwnersMutation } from '@graphql/mutations.generated';
import { FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '@types';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
    associatedUrn?: string;
}

export default function useOwnershipPrompt({ prompt, submitResponse, field, associatedUrn }: Props) {
    const { refetch: refetchSchema } = useGetEntityWithSchema(!SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type));
    const [hasEdited, setHasEdited] = useState(false);
    const { entityData } = useEntityData();
    const urn = useMutationUrn();
    const promptAssociation = getPromptAssociation(entityData, prompt.id);
    const [removeOwnersMutation] = useBatchRemoveOwnersMutation();

    const {
        form: { formView },
    } = useEntityFormContext();

    const initialOwnershipTypeUrn = useMemo(() => {
        if (formView !== FormView.BY_ENTITY) {
            return undefined;
        }
        return (
            promptAssociation?.response?.ownershipResponse?.ownershipTypeUrn ||
            getDefaultOwnershipTypeUrn(entityData, prompt)
        );
    }, [formView, promptAssociation?.response?.ownershipResponse?.ownershipTypeUrn, prompt, entityData]);

    const initialEntities = useMemo(
        () =>
            formView === FormView.BY_ENTITY
                ? promptAssociation?.response?.ownershipResponse?.owners ||
                  getDefaultOwnerEntities(entityData, prompt, initialOwnershipTypeUrn) ||
                  []
                : [],
        [formView, promptAssociation?.response?.ownershipResponse?.owners, entityData, prompt, initialOwnershipTypeUrn],
    );

    const initialValues = useMemo(
        () =>
            formView === FormView.BY_ENTITY || formView === FormView.BULK_VERIFY
                ? initialEntities.map((o) => o.urn) || []
                : [],
        [formView, initialEntities],
    );

    const [selectedValues, setSelectedValues] = useState<any[]>(initialValues);
    const [selectedOwnerTypeUrn, setSelectedOwnerTypeUrn] = useState<string | undefined>(initialOwnershipTypeUrn);

    useEffect(() => {
        if (!hasEdited && !isEqual(selectedValues, initialValues)) {
            setSelectedValues(initialValues);
        }
    }, [hasEdited, selectedValues, initialValues]);

    useEffect(() => {
        if (!hasEdited && !isEqual(selectedOwnerTypeUrn, initialOwnershipTypeUrn)) {
            setSelectedOwnerTypeUrn(initialOwnershipTypeUrn);
        }
    }, [hasEdited, selectedOwnerTypeUrn, initialOwnershipTypeUrn]);

    function updateSelectedValues(values: any[]) {
        setSelectedValues(values);
        setHasEdited(true);
    }

    function updateSelectedOwnerTypeUrn(typeUrn: string) {
        setSelectedOwnerTypeUrn(typeUrn);
        setHasEdited(true);
    }

    const finalSubmit = useCallback(
        (owners: string[], ownershipTypeUrn: string) => {
            submitResponse(
                {
                    promptId: prompt.id,
                    formUrn: prompt.formUrn,
                    type: FormPromptType.Ownership,
                    fieldPath: field?.fieldPath,
                    ownershipParams: {
                        owners,
                        ownershipTypeUrn,
                    },
                },
                () => {
                    setHasEdited(false);
                    if (field) {
                        refetchSchema();
                    }
                },
            );
        },
        [prompt, refetchSchema, field, submitResponse],
    );

    const submitOwnershipResponse = useCallback(() => {
        if (selectedValues.length && selectedOwnerTypeUrn) {
            const currentOwnersOfType = entityData?.ownership?.owners?.filter(
                (owner) => owner.ownershipType?.urn === selectedOwnerTypeUrn,
            );
            const ownersToRemove =
                currentOwnersOfType?.filter((owner) => !selectedValues.includes(owner.owner.urn)) || [];

            if (ownersToRemove.length > 0) {
                removeOwnersMutation({
                    variables: {
                        input: {
                            ownerUrns: ownersToRemove.map((o) => o.owner.urn),
                            ownershipTypeUrn: selectedOwnerTypeUrn,
                            resources: [{ resourceUrn: associatedUrn || urn }],
                        },
                    },
                }).finally(() => finalSubmit(selectedValues, selectedOwnerTypeUrn));
            } else {
                finalSubmit(selectedValues, selectedOwnerTypeUrn);
            }
        }
    }, [selectedValues, selectedOwnerTypeUrn, entityData, associatedUrn, finalSubmit, removeOwnersMutation, urn]);

    return {
        hasEdited,
        selectedValues,
        selectedOwnerTypeUrn,
        initialEntities,
        updateSelectedOwnerTypeUrn,
        submitOwnershipResponse,
        updateSelectedValues,
    };
}
