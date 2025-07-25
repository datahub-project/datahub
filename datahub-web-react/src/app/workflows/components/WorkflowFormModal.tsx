import { Button, Modal } from '@components';
import { Form } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { AccessFields } from '@app/workflows/components/AccessFields';
import { DescriptionField } from '@app/workflows/components/DescriptionField';
import { EntitySelector } from '@app/workflows/components/EntitySelector';
import { type ReviewContextData, ReviewerContext } from '@app/workflows/components/ReviewerContext';
import { WorkflowFormField } from '@app/workflows/components/WorkflowFormField';
import { useWorkflowFormCompletion } from '@app/workflows/hooks/useWorkflowFormCompletion';
import { shouldDisplayField } from '@app/workflows/utils/fieldConditions';
import { createReadOnlyFormState } from '@app/workflows/utils/fieldValueConversion';
import {
    WorkflowRequestFormModalMode,
    createFormValuesForVisibility,
    getModalConfig,
    isEntitySelectionRequired,
} from '@app/workflows/utils/modalHelpers';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';

const ModalContent = styled.div`
    max-height: 60vh;
    overflow-y: auto;
    padding: 0px 4px;

    /* Hide scrollbar for Firefox */
    scrollbar-width: none;

    /* Hide scrollbar for Webkit browsers (Chrome, Safari, Edge) */
    &::-webkit-scrollbar {
        display: none;
    }
`;

const FormSection = styled.div`
    margin-bottom: 24px;
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    margin-top: 24px;
    padding-top: 16px;
    border-top: 1px solid #f0f0f0;
`;

type Props = {
    workflow: ActionWorkflowFragment;
    entityUrn?: string;
    open: boolean;
    onClose: () => void;
    onSuccess?: (requestUrn: string) => void;
    // Mode-based props
    mode?: WorkflowRequestFormModalMode;
    initialFormData?: {
        description?: string;
        expiresAt?: number;
        fieldValues?: Record<string, any[]>;
    };
    reviewContext?: ReviewContextData;
};

export const WorkflowFormModal: React.FC<Props> = ({
    workflow,
    entityUrn,
    open,
    onClose,
    onSuccess,
    mode = WorkflowRequestFormModalMode.CREATE,
    initialFormData,
    reviewContext,
}) => {
    // Entity selection state for workflows that require entity selection
    const [selectedEntityUrn, setSelectedEntityUrn] = useState<string>(entityUrn || '');

    // Use the selected entity URN if available, otherwise fall back to prop
    const finalEntityUrn = selectedEntityUrn || entityUrn;

    // Form state management
    const hookResult = useWorkflowFormCompletion({
        workflow,
        entityUrn: finalEntityUrn,
        onSuccess: (requestUrn) => {
            hookResult.resetForm();
            setSelectedEntityUrn('');
            onSuccess?.(requestUrn);
            onClose();
        },
        onError: (error) => {
            console.error('Workflow submission error:', error);
        },
    });

    // Use read-only state in review mode, otherwise use hook state
    const formStateResult =
        mode === WorkflowRequestFormModalMode.REVIEW ? createReadOnlyFormState(workflow, initialFormData) : hookResult;

    const {
        formState,
        description,
        setDescription,
        expiresAt,
        setExpiresAt,
        isSubmitting,
        updateFieldValue,
        submitWorkflow,
        resetForm,
    } = formStateResult;

    // Create form values in the format expected by shouldDisplayField
    const currentFormValues = useMemo(() => createFormValuesForVisibility(formState || {}), [formState]);

    // Filter fields based on their display conditions
    const visibleFields = useMemo(() => {
        return workflow.trigger?.form?.fields.filter((field) => shouldDisplayField(field, currentFormValues)) || [];
    }, [workflow.trigger?.form?.fields, currentFormValues]);

    // Get modal configuration
    const modalConfig = getModalConfig(mode, workflow);

    const handleCancel = () => {
        if (mode !== WorkflowRequestFormModalMode.REVIEW) {
            resetForm();
            setSelectedEntityUrn('');
        }
        onClose();
    };

    const handleSubmit = async () => {
        await submitWorkflow();
    };

    const isEntityRequired = isEntitySelectionRequired(workflow, selectedEntityUrn);
    const isDisabled = isSubmitting || mode === WorkflowRequestFormModalMode.REVIEW;

    return (
        <Modal
            title={modalConfig.title}
            subtitle={workflow.description || undefined}
            open={open}
            onCancel={handleCancel}
            footer={null}
            width={600}
            destroyOnClose
            buttons={[]}
            data-testid="workflow-completion-modal"
        >
            <ModalContent onClick={(e) => e.stopPropagation()}>
                <Form layout="vertical" data-testid="workflow-form">
                    {/* Reviewer Context - only show in review mode */}
                    {mode === WorkflowRequestFormModalMode.REVIEW && reviewContext && (
                        <ReviewerContext reviewContext={reviewContext} />
                    )}

                    {/* Entity Selection - show if workflow requires entity selection */}
                    <EntitySelector
                        workflow={workflow}
                        selectedEntityUrn={selectedEntityUrn}
                        onEntityChange={setSelectedEntityUrn}
                        mode={mode}
                        disabled={isSubmitting}
                    />

                    {/* Required workflow fields - filtered by conditions */}
                    {visibleFields.length > 0 && (
                        <FormSection>
                            {visibleFields.map((field) => {
                                const fieldState = formState?.[field.id];
                                return (
                                    <WorkflowFormField
                                        key={field.id}
                                        field={field}
                                        values={fieldState?.values || []}
                                        error={fieldState?.error}
                                        onChange={(values) => updateFieldValue(field.id, values)}
                                        disabled={isDisabled}
                                        isReviewMode={mode === WorkflowRequestFormModalMode.REVIEW}
                                    />
                                );
                            })}
                        </FormSection>
                    )}

                    {/* Access-specific fields */}
                    <AccessFields
                        workflow={workflow}
                        expiresAt={expiresAt}
                        onExpiresAtChange={setExpiresAt}
                        mode={mode}
                        disabled={isSubmitting}
                    />

                    {/* Optional description */}
                    <DescriptionField
                        description={description}
                        onDescriptionChange={setDescription}
                        mode={mode}
                        disabled={isSubmitting}
                    />
                </Form>
            </ModalContent>

            <ModalFooter>
                <Button
                    variant="text"
                    color="gray"
                    onClick={handleCancel}
                    disabled={isSubmitting}
                    data-testid="workflow-cancel-button"
                >
                    {modalConfig.cancelButtonText}
                </Button>
                {mode !== WorkflowRequestFormModalMode.REVIEW && (
                    <Button
                        variant="filled"
                        color="primary"
                        onClick={handleSubmit}
                        isLoading={isSubmitting}
                        disabled={isSubmitting || isEntityRequired || false}
                        data-testid="workflow-submit-button"
                    >
                        {modalConfig.submitButtonText}
                    </Button>
                )}
            </ModalFooter>
        </Modal>
    );
};
