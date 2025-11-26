import React, { useEffect } from 'react';

import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function SelectSourceStep() {
    const { updateState, setCurrentStepCompleted, isCurrentStepCompleted } = useMultiStepContext<
        SourceBuilderState,
        IngestionSourceFormStep
    >();

    useEffect(() => {
        if (!isCurrentStepCompleted()) {
            setCurrentStepCompleted();
            updateState({
                type: 'cassandra',
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return <>Select Source Step</>;
}
