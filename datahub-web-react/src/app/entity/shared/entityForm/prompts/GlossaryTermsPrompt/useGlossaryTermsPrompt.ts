import { isEqual } from 'lodash';
import { useEffect, useMemo, useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getPromptAssociation } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '@app/entity/shared/entityForm/constants';
import { useGetDefaultTerms } from '@app/entity/shared/entityForm/prompts/GlossaryTermsPrompt/useGetDefaultTerms';
import { useGetEntityWithSchema } from '@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema';
import usePrevious from '@src/app/shared/usePrevious';

import { EditableSchemaMetadata, FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '@types';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
}

export default function useGlossaryTermsPrompt({ prompt, submitResponse, field }: Props) {
    const { refetch: refetchSchema, entityWithSchema } = useGetEntityWithSchema(
        !SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type),
    );

    const [hasEdited, setHasEdited] = useState(false);
    const { entityData } = useEntityData();
    const promptAssociation = getPromptAssociation(entityData, prompt.id);
    const completedFieldAssociation = promptAssociation?.fieldAssociations?.completedFieldPrompts?.find(
        (p) => p.fieldPath === field?.fieldPath,
    );

    const {
        form: { formView },
    } = useEntityFormContext();

    const entityDefaultTerms = useGetDefaultTerms(entityData, prompt);

    const fieldDefaultTerms = useGetDefaultTerms(
        entityData,
        prompt,
        field,
        entityWithSchema?.editableSchemaMetadata as EditableSchemaMetadata,
    );

    const initialEntities = useMemo(() => {
        if (formView !== FormView.BY_ENTITY) {
            return [];
        }
        return field
            ? completedFieldAssociation?.response?.glossaryTermsResponse?.glossaryTerms || fieldDefaultTerms || []
            : promptAssociation?.response?.glossaryTermsResponse?.glossaryTerms || entityDefaultTerms || [];
    }, [
        formView,
        promptAssociation?.response?.glossaryTermsResponse?.glossaryTerms,
        completedFieldAssociation?.response?.glossaryTermsResponse?.glossaryTerms,
        field,
        entityDefaultTerms,
        fieldDefaultTerms,
    ]);
    const initialValues = useMemo(() => initialEntities.map((e) => e.urn), [initialEntities]);

    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues);
    const previousInitial = usePrevious(initialValues);

    useEffect(() => {
        if (!isEqual(previousInitial, initialValues)) {
            setSelectedValues(initialValues);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [initialValues]);

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
                    type: field ? FormPromptType.FieldsGlossaryTerms : FormPromptType.GlossaryTerms,
                    fieldPath: field?.fieldPath,
                    glossaryTermsParams: {
                        glossaryTermUrns: selectedValues,
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
        initialEntities,
        hasEdited,
        selectedValues,
        submitGlossaryTermsResponse,
        updateSelectedValues,
    };
}
