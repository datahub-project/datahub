import React, { useCallback } from 'react';

import { IngestionSourceBuilderLayout } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilderLayout';
import { IngestionSourceForm } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceForm';
import { MultiStepSourceBuilderState, SubmitOptions } from '@app/ingestV2/source/multiStepBuilder/types';
import { useDiscardUnsavedChangesConfirmationContext } from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { MultiStepFormProviderProps, OnCancelArguments } from '@app/sharedV2/forms/multiStepForm/types';

export function IngestionSourceBuilder(props: MultiStepFormProviderProps<MultiStepSourceBuilderState, SubmitOptions>) {
    const { onCancel } = props;
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
            {...props}
            onCancel={onCancelWithDiscardUnsavedChangesConfirmation}
        >
            <IngestionSourceBuilderLayout>
                <IngestionSourceForm />
            </IngestionSourceBuilderLayout>
        </MultiStepFormProvider>
    );
}
