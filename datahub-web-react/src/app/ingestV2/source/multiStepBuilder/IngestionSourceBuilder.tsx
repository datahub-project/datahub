import React, { useCallback } from 'react';

import { IngestionSourceBuilderLayout } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilderLayout';
import { IngestionSourceForm } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceForm';
import { MultiStepSourceBuilderState, SubmitOptions } from '@app/ingestV2/source/multiStepBuilder/types';
import { useDiscardUnsavedChangesConfirmationContext } from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { MultiStepFormProviderProps, OnCancelArguments } from '@app/sharedV2/forms/multiStepForm/types';

interface IngestionSourceBuilderProps extends MultiStepFormProviderProps<MultiStepSourceBuilderState, SubmitOptions> {
    isEditing?: boolean;
    sourceUrn?: string;
}

export function IngestionSourceBuilder({
    isEditing,
    sourceUrn,
    onCancel,
    ...providerProps
}: IngestionSourceBuilderProps) {
    const { showConfirmation } = useDiscardUnsavedChangesConfirmationContext();

    const onCancelWithDiscardUnsavedChangesConfirmation = useCallback(
        (args: OnCancelArguments) => {
            if (args.isDirty) {
                showConfirmation({ onConfirm: () => onCancel?.(args) });
            } else {
                onCancel?.(args);
            }
        },
        [onCancel, showConfirmation],
    );

    return (
        <MultiStepFormProvider<MultiStepSourceBuilderState, SubmitOptions>
            {...providerProps}
            onCancel={onCancelWithDiscardUnsavedChangesConfirmation}
        >
            <IngestionSourceBuilderLayout isEditing={isEditing} sourceUrn={sourceUrn}>
                <IngestionSourceForm />
            </IngestionSourceBuilderLayout>
        </MultiStepFormProvider>
    );
}
