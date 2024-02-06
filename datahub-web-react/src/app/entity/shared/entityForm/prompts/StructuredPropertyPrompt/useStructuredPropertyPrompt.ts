import { useEffect, useMemo, useState } from 'react';
import { useEntityContext } from '../../../EntityContext';
import { FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '../../../../../../types.generated';
import { getInitialValues } from './utils';
import usePrevious from '../../../../../shared/usePrevious';
import { useGetEntityWithSchema } from '../../../tabs/Dataset/Schema/useGetEntitySchema';
import { FormView, useEntityFormContext } from '../../EntityFormContext';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
}

export default function useStructuredPropertyPrompt({ prompt, submitResponse, field }: Props) {
    const { refetch: refetchSchema } = useGetEntityWithSchema();
    const { refetch, entityData } = useEntityContext();
    const { selectedPromptId, formView } = useEntityFormContext();
    const [isSaveVisible, setIsSaveVisible] = useState(false);
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
            setIsSaveVisible(false);
            setSelectedValues(initialValues || []);
        }
    }, [previousSelectedPromptId, selectedPromptId, initialValues]);

    // respond to prompts
    function selectSingleValue(value: string | number) {
        setIsSaveVisible(true);
        setSelectedValues([value as string]);
    }

    function toggleSelectedValue(value: string | number) {
        setIsSaveVisible(true);
        if (selectedValues.includes(value)) {
            setSelectedValues((prev) => prev.filter((v) => v !== value));
        } else {
            setSelectedValues((prev) => [...prev, value]);
        }
    }

    function updateSelectedValues(values: any[]) {
        setSelectedValues(values);
        setIsSaveVisible(true);
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
                setIsSaveVisible(false);
                if (field) {
                    refetchSchema();
                }
            },
        );
    }

    return {
        isSaveVisible,
        selectedValues,
        selectSingleValue,
        toggleSelectedValue,
        submitStructuredPropertyResponse,
        updateSelectedValues,
    };
}
