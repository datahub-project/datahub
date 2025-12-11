import React, { useEffect } from 'react';

import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function ScheduleStep() {
    const { updateState, setCurrentStepCompleted, isCurrentStepCompleted } = useMultiStepContext<
        MultiStepSourceBuilderState,
        IngestionSourceFormStep
    >();

    useEffect(() => {
        if (!isCurrentStepCompleted()) {
            setCurrentStepCompleted();
            updateState({
                schedule: {
                    timezone: 'Europe/Minsk',
                    interval: '0 0 * * *',
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return <>Schedule Step</>;
}
