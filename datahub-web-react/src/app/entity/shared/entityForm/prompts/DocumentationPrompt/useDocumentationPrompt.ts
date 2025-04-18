import { isEqual } from 'lodash';
import { useEffect, useMemo, useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getPromptAssociation } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '@app/entity/shared/entityForm/constants';
import { getInitialDocumentationValues } from '@app/entity/shared/entityForm/prompts/DocumentationPrompt/utils';
import { useGetEntityWithSchema } from '@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema';
import { useShouldShowInferDocumentationButton } from '@src/app/entityV2/shared/components/inferredDocs/utils';
import usePrevious from '@src/app/shared/usePrevious';

import { EditableSchemaFieldInfo, FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '@types';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
}

export default function useDocumentationPrompt({ prompt, submitResponse, field }: Props) {
    const { refetch: refetchSchema, entityWithSchema } = useGetEntityWithSchema(
        !SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type),
    );
    const [hasEdited, setHasEdited] = useState(false);
    const { entityData, entityType } = useEntityData();
    const promptAssociation = getPromptAssociation(entityData, prompt.id);
    const completedFieldAssociation = promptAssociation?.fieldAssociations?.completedFieldPrompts?.find(
        (p) => p.fieldPath === field?.fieldPath,
    );

    const {
        form: { formView },
    } = useEntityFormContext();

    const enableInferredDescriptions = useShouldShowInferDocumentationButton(entityType);

    const editableFieldInfo = entityWithSchema?.editableSchemaMetadata?.editableSchemaFieldInfo.find(
        (info) => info.fieldPath === field?.fieldPath || undefined,
    ) as EditableSchemaFieldInfo | undefined;

    const initialValue = useMemo(
        () =>
            formView === FormView.BY_ENTITY
                ? getInitialDocumentationValues(
                      entityData,
                      field,
                      promptAssociation,
                      completedFieldAssociation,
                      editableFieldInfo,
                      enableInferredDescriptions,
                  )
                : '',

        [
            formView,
            promptAssociation,
            completedFieldAssociation,
            entityData,
            field,
            editableFieldInfo,
            enableInferredDescriptions,
        ],
    );

    const [documentationValue, setDocumentationValue] = useState<string>(initialValue);
    const previousInitial = usePrevious(initialValue);

    useEffect(() => {
        if (!isEqual(previousInitial, initialValue)) setDocumentationValue(initialValue);
    }, [initialValue, previousInitial]);

    function updateDocumentation(value: string) {
        if (value !== initialValue) {
            setDocumentationValue(value);
            setHasEdited(true);
        }
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
