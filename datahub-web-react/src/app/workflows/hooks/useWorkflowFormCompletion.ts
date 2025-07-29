import { message } from 'antd';
import { useCallback, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { convertFormStateToRequestFields } from '@app/workflows/utils/fieldValueConversion';
import {
    type FieldValue,
    type FormFieldState,
    type WorkflowFormState,
    applyFormFieldErrors,
    createInitialFormState,
    resetFormState,
    setFormFieldError,
    updateFormFieldValue,
} from '@app/workflows/utils/formStateHelpers';
import { validateWorkflowForm } from '@app/workflows/utils/formValidation';

import { ActionWorkflowFragment, useCreateActionWorkflowFormRequestMutation } from '@graphql/actionWorkflow.generated';

// Re-export types for backward compatibility
export type { FieldValue, FormFieldState, WorkflowFormState };

export type UseWorkflowFormCompletionOptions = {
    workflow: ActionWorkflowFragment;
    entityUrn?: string;
    onSuccess?: (requestUrn: string) => void;
    onError?: (error: any) => void;
};

/**
 * Hook for managing workflow completion state and submission
 */
export const useWorkflowFormCompletion = (options: UseWorkflowFormCompletionOptions) => {
    const { workflow, entityUrn, onSuccess, onError } = options;

    const [formState, setFormState] = useState<WorkflowFormState>(() =>
        createInitialFormState(workflow.trigger?.form?.fields || []),
    );

    const [description, setDescription] = useState<string>('');
    const [expiresAt, setExpiresAt] = useState<number | undefined>(undefined);
    const [isSubmitting, setIsSubmitting] = useState(false);

    const [createWorkflowFormRequest] = useCreateActionWorkflowFormRequestMutation();

    const updateFieldValue = useCallback((fieldId: string, values: FieldValue[]) => {
        setFormState((prev) => updateFormFieldValue(prev, fieldId, values));
    }, []);

    const setFieldError = useCallback((fieldId: string, error: string) => {
        setFormState((prev) => setFormFieldError(prev, fieldId, error));
    }, []);

    const validateForm = useCallback((): boolean => {
        const validationResult = validateWorkflowForm(workflow.trigger?.form?.fields || [], formState);

        if (!validationResult.isValid) {
            setFormState((prev) => applyFormFieldErrors(prev, validationResult.fieldErrors));
        }

        return validationResult.isValid;
    }, [workflow.trigger?.form?.fields, formState]);

    const submitWorkflow = useCallback(async () => {
        if (!validateForm()) {
            console.log('❌ Form validation failed, aborting submission');
            return;
        }
        setIsSubmitting(true);

        try {
            const fields = convertFormStateToRequestFields(workflow.trigger?.form?.fields || [], formState);

            const input = {
                workflowUrn: workflow.urn,
                description: description || undefined,
                fields,
                ...(entityUrn && { entityUrn }),
                ...(workflow.category === 'ACCESS' &&
                    expiresAt && {
                        access: { expiresAt },
                    }),
            };

            const result = await createWorkflowFormRequest({
                variables: { input },
            });

            const requestUrn = result.data?.createActionWorkflowFormRequest;
            if (requestUrn) {
                message.success(
                    `${workflow.name} request submitted successfully! View your open requests in your Tasks center.`,
                );
                onSuccess?.(requestUrn);
                analytics.event({
                    type: EventType.CreateActionWorkflowFormRequest,
                    workflowUrn: workflow.urn,
                    workflowName: workflow.name,
                    workflowCategory: workflow.category,
                    actionRequestUrn: requestUrn,
                });
            }
        } catch (error) {
            console.error('❌ Error submitting workflow request:', error);
            message.error('Failed to submit workflow request. Please try again.');
            onError?.(error);
        } finally {
            setIsSubmitting(false);
        }
    }, [
        validateForm,
        workflow,
        formState,
        description,
        entityUrn,
        expiresAt,
        createWorkflowFormRequest,
        onSuccess,
        onError,
    ]);

    const resetForm = useCallback(() => {
        setFormState(resetFormState(workflow.trigger?.form?.fields || []));
        setDescription('');
        setExpiresAt(undefined);
    }, [workflow.trigger?.form?.fields]);

    return {
        formState,
        description,
        setDescription,
        expiresAt,
        setExpiresAt,
        isSubmitting,
        updateFieldValue,
        setFieldError,
        submitWorkflow,
        resetForm,
        isValid: validateForm,
    };
};
