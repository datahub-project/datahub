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

export default function useGlossaryTermsPrompt({ prompt, submitResponse, field }: Props) {
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

    const initialEntities = useMemo(
        () =>
            formView === FormView.BY_ENTITY && field
                ? completedFieldAssociation?.response?.glossaryTermsResponse?.glossaryTerms || []
                : promptAssociation?.response?.glossaryTermsResponse?.glossaryTerms || [],
        [
            formView,
            promptAssociation?.response?.glossaryTermsResponse?.glossaryTerms,
            completedFieldAssociation?.response?.glossaryTermsResponse?.glossaryTerms,
            field,
        ],
    );
    const initialValues = useMemo(() => initialEntities.map((e) => e.urn), [initialEntities]);

    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues);

    function updateSelectedValues(values: any[]) {
        setSelectedValues(values);
        setHasEdited(true);
    }

    function submitGlossaryTermsResponse() {
        if (selectedValues.length) {
            submitResponse(
                {
                    promptId: prompt.id,
                    formUrn: prompt.formUrn,
                    type: field ? FormPromptType.FieldsDocumentation : FormPromptType.Documentation,
                    fieldPath: field?.fieldPath,
                    // TODO: accept response
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
        initialEntities,
        hasEdited,
        selectedValues,
        submitGlossaryTermsResponse,
        updateSelectedValues,
    };
}
