import { useEffect, useMemo, useState } from 'react';
import { useEntityContext } from '../../../EntityContext';
import { FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '../../../../../../types.generated';
import { getInitialValues } from './utils';
import usePrevious from '../../../../../shared/usePrevious';
import { useGetEntityWithSchema } from '../../../tabs/Dataset/Schema/useGetEntitySchema';
import { FormView, useEntityFormContext } from '../../EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '../../constants';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
}

export default function useStructuredPropertyPrompt({ prompt, submitResponse, field }: Props) {
    const { refetch: refetchSchema } = useGetEntityWithSchema(!SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type));
    const { refetch, entityData } = useEntityContext();
    const { selectedPromptId, formView } = useEntityFormContext();
    const [hasEditedPrompt, setHasEditedPrompt] = useState(false);
    const initialValues = useMemo(
        () => (formView === FormView.BY_ENTITY ? getInitialValues(prompt, entityData, field) : []),
        [formView, entityData, prompt, field],
    );
    const [selectedValues, setSelectedValues] = useState<any[]>(initialValues || []);

    const structuredProperty = prompt.structuredPropertyParams?.structuredProperty;

    const previousEntityUrn = usePrevious(entityData?.urn);
    useEffect(() => {
        if (entityData?.urn !== previousEntityUrn) {
            setSelectedValues(initialValues || []);
        }
    }, [entityData?.urn, previousEntityUrn, initialValues]);

    const previousSelectedPromptId = usePrevious(selectedPromptId);
    useEffect(() => {
        if (selectedPromptId !== previousSelectedPromptId) {
            setHasEditedPrompt(false);
            setSelectedValues(initialValues || []);
        }
    }, [previousSelectedPromptId, selectedPromptId, initialValues]);

    // respond to prompts
    function selectSingleValue(value: string | number) {
        setHasEditedPrompt(true);
        setSelectedValues([value as string]);
    }

    function toggleSelectedValue(value: string | number) {
        setHasEditedPrompt(true);
        if (selectedValues.includes(value)) {
            setSelectedValues((prev) => prev.filter((v) => v !== value));
        } else {
            setSelectedValues((prev) => [...prev, value]);
        }
    }

    function updateSelectedValues(values: any[]) {
        setSelectedValues(values);
        setHasEditedPrompt(true);
    }

    // submit structured property prompt
    function submitStructuredPropertyResponse() {
        submitResponse(
            {
                promptId: prompt.id,
                formUrn: prompt.formUrn,
                type: field ? FormPromptType.FieldsStructuredProperty : FormPromptType.StructuredProperty,
                fieldPath: field?.fieldPath,
                structuredPropertyParams: {
                    structuredPropertyUrn: structuredProperty?.urn as string,
                    values: selectedValues.map((value) => {
                        if (typeof value === 'string') {
                            return { stringValue: value as string };
                        }
                        return { numberValue: value as number };
                    }),
                },
            },
            () => {
                refetch();
                setHasEditedPrompt(false);
                if (field) {
                    refetchSchema();
                }
            },
        );
    }

    return {
        hasEditedPrompt,
        selectedValues,
        selectSingleValue,
        toggleSelectedValue,
        submitStructuredPropertyResponse,
        updateSelectedValues,
    };
}
