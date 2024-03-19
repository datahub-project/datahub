import { useEffect, useMemo } from 'react';
import { useEntityContext } from '../../../EntityContext';
import { FormPrompt, FormPromptType, SchemaField, SubmitFormPromptInput } from '../../../../../../types.generated';
import { getInitialValues } from './utils';
import usePrevious from '../../../../../shared/usePrevious';
import { useGetEntityWithSchema } from '../../../tabs/Dataset/Schema/useGetEntitySchema';
import { FormView, useEntityFormContext } from '../../EntityFormContext';
import { SCHEMA_FIELD_PROMPT_TYPES } from '../../constants';
import { useEditStructuredProperty } from '../../../components/styled/StructuredProperty/useEditStructuredProperty';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
}

export default function useStructuredPropertyPrompt({ prompt, submitResponse, field }: Props) {
    const { refetch: refetchSchema } = useGetEntityWithSchema(!SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type));
    const { refetch, entityData } = useEntityContext();
    const { selectedPromptId, formView } = useEntityFormContext();
    const initialValues = useMemo(
        () => (formView === FormView.BY_ENTITY ? getInitialValues(prompt, entityData, field) : []),
        [formView, entityData, prompt, field],
    );
    const {
        selectedValues,
        setSelectedValues,
        selectSingleValue,
        toggleSelectedValue,
        updateSelectedValues,
        hasEdited,
        setHasEdited,
    } = useEditStructuredProperty();

    const structuredProperty = prompt.structuredPropertyParams?.structuredProperty;

    const previousEntityUrn = usePrevious(entityData?.urn);
    useEffect(() => {
        if (entityData?.urn !== previousEntityUrn) {
            setSelectedValues(initialValues || []);
        }
    }, [entityData?.urn, previousEntityUrn, initialValues, setSelectedValues]);

    const previousSelectedPromptId = usePrevious(selectedPromptId);
    useEffect(() => {
        if (selectedPromptId !== previousSelectedPromptId) {
            setHasEdited(false);
            setSelectedValues(initialValues || []);
        }
    }, [previousSelectedPromptId, selectedPromptId, initialValues, setSelectedValues, setHasEdited]);

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
                setHasEdited(false);
                if (field) {
                    refetchSchema();
                }
            },
        );
    }

    return {
        hasEdited,
        selectedValues,
        selectSingleValue,
        toggleSelectedValue,
        submitStructuredPropertyResponse,
        updateSelectedValues,
    };
}
