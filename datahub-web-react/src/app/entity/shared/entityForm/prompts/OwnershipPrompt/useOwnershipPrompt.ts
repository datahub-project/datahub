import { useMemo, useState } from 'react';
import { FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '../../../../../../types.generated';
import { useEntityData } from '../../../EntityContext';
import { getPromptAssociation } from '../../../containers/profile/sidebar/FormInfo/utils';
import { useGetEntityWithSchema } from '../../../tabs/Dataset/Schema/useGetEntitySchema';
import { FormView, useEntityFormContext } from '../../EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '../../constants';
import { getDefaultOwnerEntities, getDefaultOwnershipTypeUrn } from './utils';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
}

export default function useOwnershipPrompt({ prompt, submitResponse, field }: Props) {
    const { refetch: refetchSchema } = useGetEntityWithSchema(!SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type));
    const [hasEdited, setHasEdited] = useState(false);
    const { entityData } = useEntityData();
    const promptAssociation = getPromptAssociation(entityData, prompt.id);

    const {
        form: { formView },
    } = useEntityFormContext();

    const initialEntities = useMemo(
        () =>
            formView === FormView.BY_ENTITY
                ? promptAssociation?.response?.ownershipResponse?.owners ||
                  getDefaultOwnerEntities(entityData, prompt) ||
                  []
                : [],
        [formView, promptAssociation?.response?.ownershipResponse?.owners, entityData, prompt],
    );

    const initialValues = useMemo(
        () =>
            formView === FormView.BY_ENTITY || formView === FormView.BULK_VERIFY
                ? initialEntities.map((o) => o.urn) || []
                : [],
        [formView, initialEntities],
    );

    const initialOwnershipTypeUrn = useMemo(() => {
        if (formView !== FormView.BY_ENTITY) {
            return undefined;
        }
        return (
            promptAssociation?.response?.ownershipResponse?.ownershipTypeUrn ||
            getDefaultOwnershipTypeUrn(entityData, prompt, initialValues)
        );
    }, [formView, promptAssociation?.response?.ownershipResponse?.ownershipTypeUrn, prompt, entityData, initialValues]);

    const [selectedValues, setSelectedValues] = useState<any[]>(initialValues);
    const [selectedOwnerTypeUrn, setSelectedOwnerTypeUrn] = useState<string | undefined>(initialOwnershipTypeUrn);

    function updateSelectedValues(values: any[]) {
        setSelectedValues(values);
        setHasEdited(true);
    }

    function updateSelectedOwnerTypeUrn(urn: string) {
        setSelectedOwnerTypeUrn(urn);
        setHasEdited(true);
    }

    function submitOwnershipResponse() {
        if (selectedValues.length && selectedOwnerTypeUrn) {
            submitResponse(
                {
                    promptId: prompt.id,
                    formUrn: prompt.formUrn,
                    type: FormPromptType.Ownership,
                    fieldPath: field?.fieldPath,
                    ownershipParams: {
                        owners: selectedValues,
                        ownershipTypeUrn: selectedOwnerTypeUrn,
                    },
                },
                () => {
                    setHasEdited(false);
                    if (field) {
                        refetchSchema();
                    }
                },
            );
        }
    }

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
