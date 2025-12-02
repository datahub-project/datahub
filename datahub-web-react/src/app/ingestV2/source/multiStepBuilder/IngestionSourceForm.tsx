import React, { useMemo } from 'react';

import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function IngestionSourceForm() {
    const { getCurrentStep } = useMultiStepContext();

    const currentStep = useMemo(() => getCurrentStep?.(), [getCurrentStep]);

    if (!currentStep) return null;

    return <>{currentStep.content}</>;
}
