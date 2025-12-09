import React from 'react';

import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { IngestionSourceBuilderLayout } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilderLayout';
import { IngestionSourceForm } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceForm';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { MultiStepFormProviderProps } from '@app/sharedV2/forms/multiStepForm/types';

interface IngestionSourceBuilderProps extends MultiStepFormProviderProps<SourceBuilderState> {
    isEditing?: boolean;
    sourceUrn?: string;
}

export function IngestionSourceBuilder({ isEditing, sourceUrn, ...providerProps }: IngestionSourceBuilderProps) {
    return (
        <MultiStepFormProvider<SourceBuilderState> {...providerProps}>
            <IngestionSourceBuilderLayout isEditing={isEditing} sourceUrn={sourceUrn}>
                <IngestionSourceForm />
            </IngestionSourceBuilderLayout>
        </MultiStepFormProvider>
    );
}
