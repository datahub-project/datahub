import { useMemo, useState } from 'react';
import { useEntityData } from '../../../EntityContext';
import { FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '../../../../../../types.generated';
import { useGetEntityWithSchema } from '../../../tabs/Dataset/Schema/useGetEntitySchema';
import { FormView, useEntityFormContext } from '../../EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '../../constants';
import { getPromptAssociation } from '../../../containers/profile/sidebar/FormInfo/utils';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
}

export default function useDocumentationPrompt({ prompt, submitResponse, field }: Props) {
    const { refetch: refetchSchema } = useGetEntityWithSchema(!SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type));
    const [hasEdited, setHasEdited] = useState(false);
    const { entityData } = useEntityData();
    const promptAssociation = getPromptAssociation(entityData, prompt.id);
    const completedFieldAssociation = promptAssociation?.fieldAssociations?.completedFieldPrompts?.find(
        (p) => p.fieldPath === field?.fieldPath,
    );

    const {
        form: { formView },
    } = useEntityFormContext();

    const initialValue = useMemo(
        () =>
            formView === FormView.BY_ENTITY && field
                ? completedFieldAssociation?.response?.documentationResponse?.documentation || ''
                : promptAssociation?.response?.documentationResponse?.documentation || '',
        [
            formView,
            promptAssociation?.response?.documentationResponse?.documentation,
            completedFieldAssociation?.response?.documentationResponse?.documentation,
            field,
        ],
    );

    const [documentationValue, setDocumentationValue] = useState<string>(initialValue);

    function updateDocumentation(value: string) {
        setDocumentationValue(value);
        setHasEdited(true);
    }

    function submitDocumentationResponse() {
        if (documentationValue.length) {
            submitResponse(
                {
                    promptId: prompt.id,
                    formUrn: prompt.formUrn,
                    type: field ? FormPromptType.FieldsDocumentation : FormPromptType.Documentation,
                    fieldPath: field?.fieldPath,
                    documentationParams: {
                        documentation: documentationValue,
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
        documentationValue,
        submitDocumentationResponse,
        updateDocumentation,
    };
}
