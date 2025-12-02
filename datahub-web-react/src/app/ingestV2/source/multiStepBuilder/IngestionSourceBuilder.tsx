import React from 'react';

import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { IngestionSourceBuilderLayout } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilderLayout';
import { IngestionSourceForm } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceForm';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { MultiStepFormProviderProps } from '@app/sharedV2/forms/multiStepForm/types';

export function IngestionSourceBuilder(props: MultiStepFormProviderProps<SourceBuilderState>) {
    return (
        <MultiStepFormProvider<SourceBuilderState> {...props}>
            <IngestionSourceBuilderLayout>
                <IngestionSourceForm />
            </IngestionSourceBuilderLayout>
        </MultiStepFormProvider>
    );
}
