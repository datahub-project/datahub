import { useContext, useEffect, useState } from 'react';

import { STEP_STATE_COMPLETE, STEP_STATE_DISMISSED, STEP_STATE_KEY } from '@app/onboarding/configV2/FreeTrialConfig';
import { getStepPropertyByKey } from '@app/onboarding/utils';
import { useIsFreeTrialInstance } from '@app/useAppConfig';
import { EducationStepsContext } from '@providers/EducationStepsContext';

interface UseFreeTrialPopoverVisibilityOptions {
    /** Step IDs to check - if ANY of these have been seen, the popover won't show */
    stepIds: string[];
}

interface UseFreeTrialPopoverVisibilityResult {
    isVisible: boolean;
    setIsVisible: (visible: boolean) => void;
    isFreeTrialInstance: boolean;
}

/**
 * Hook to manage free trial popover visibility based on education steps.
 * Checks if the user is on a free trial instance and if any of the given step IDs
 * have already been seen.
 */
export function useFreeTrialPopoverVisibility({
    stepIds,
}: UseFreeTrialPopoverVisibilityOptions): UseFreeTrialPopoverVisibilityResult {
    const isFreeTrialInstance = useIsFreeTrialInstance();
    const { educationSteps } = useContext(EducationStepsContext);
    const [isVisible, setIsVisible] = useState(false);

    useEffect(() => {
        if (!isFreeTrialInstance) {
            setIsVisible(false);
            return;
        }

        // Check if any of the step IDs have been seen
        const hasSeenAnyStep = stepIds.some((stepId) => {
            const stepExists = educationSteps?.some((step) => step.id === stepId);
            if (stepExists) return true;

            const stepState = getStepPropertyByKey(educationSteps, stepId, STEP_STATE_KEY);
            return stepState === STEP_STATE_COMPLETE || stepState === STEP_STATE_DISMISSED;
        });

        setIsVisible(!hasSeenAnyStep);
    }, [isFreeTrialInstance, educationSteps, stepIds]);

    return {
        isVisible,
        setIsVisible,
        isFreeTrialInstance,
    };
}
