import React from 'react';

import { IngestionSourceBuilderLayout } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilderLayout';
import { IngestionSourceForm } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceForm';
import { MultiStepSourceBuilderState, SubmitOptions } from '@app/ingestV2/source/multiStepBuilder/types';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { MultiStepFormProviderProps } from '@app/sharedV2/forms/multiStepForm/types';

interface IngestionSourceBuilderProps extends MultiStepFormProviderProps<MultiStepSourceBuilderState, SubmitOptions> {
    isEditing?: boolean;
    sourceUrn?: string;
}

export function IngestionSourceBuilder({ isEditing, sourceUrn, ...providerProps }: IngestionSourceBuilderProps) {
    return (
        <MultiStepFormProvider<MultiStepSourceBuilderState, SubmitOptions> {...providerProps}>
            <IngestionSourceBuilderLayout isEditing={isEditing} sourceUrn={sourceUrn}>
                <IngestionSourceForm />
            </IngestionSourceBuilderLayout>
        </MultiStepFormProvider>
    );
}
