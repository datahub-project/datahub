import React from 'react';

import { IngestionSourceBuilderLayout } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilderLayout';
import { IngestionSourceForm } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceForm';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { MultiStepFormProviderProps } from '@app/sharedV2/forms/multiStepForm/types';

export function IngestionSourceBuilder(props: MultiStepFormProviderProps<MultiStepSourceBuilderState>) {
    return (
        <MultiStepFormProvider<MultiStepSourceBuilderState> {...props}>
            <IngestionSourceBuilderLayout>
                <IngestionSourceForm />
            </IngestionSourceBuilderLayout>
        </MultiStepFormProvider>
    );
}
