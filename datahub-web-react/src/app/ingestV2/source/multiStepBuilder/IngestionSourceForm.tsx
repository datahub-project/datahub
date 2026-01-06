import React, { useEffect, useMemo } from 'react';

import { useDiscardUnsavedChangesConfirmationContext } from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function IngestionSourceForm() {
    const { getCurrentStep, isDirty } = useMultiStepContext();

    const { setIsDirty } = useDiscardUnsavedChangesConfirmationContext();
    useEffect(() => setIsDirty(isDirty()), [isDirty, setIsDirty]);

    const currentStep = useMemo(() => getCurrentStep?.(), [getCurrentStep]);

    if (!currentStep) return null;

    return <>{currentStep.content}</>;
}
