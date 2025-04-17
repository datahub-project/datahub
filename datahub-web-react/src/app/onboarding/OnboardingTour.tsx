import { Button } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import Tour from 'reactour';

import { useUserContext } from '@app/context/useUserContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import useShouldSkipOnboardingTour from '@app/onboarding/useShouldSkipOnboardingTour';
import { convertStepId, getConditionalStepIdsToAdd, getStepsToRender } from '@app/onboarding/utils';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';
import { StepStateResult } from '@types';

type Props = {
    stepIds: string[];
};

export const OnboardingTour = ({ stepIds }: Props) => {
    const { educationSteps, setEducationSteps, educationStepIdsAllowlist } = useContext(EducationStepsContext);
    const userUrn = useUserContext()?.user?.urn;
    const [isOpen, setIsOpen] = useState(true);
    const [reshow, setReshow] = useState(false);
    const isThemeV2 = useIsThemeV2();
    const accentColor = isThemeV2 ? REDESIGN_COLORS.BACKGROUND_PURPLE : '#5cb7b7';

    useEffect(() => {
        function handleKeyDown(e) {
            // Allow reshow if Cmnd + Ctrl + T is pressed
            if (e.metaKey && e.ctrlKey && e.key === 't') {
                setReshow(true);
                setIsOpen(true);
            }
            if (e.metaKey && e.ctrlKey && e.key === 'h') {
                setReshow(false);
                setIsOpen(false);
            }
        }
        document.addEventListener('keydown', handleKeyDown);
    }, []);

    const steps = getStepsToRender(educationSteps, stepIds, userUrn || '', reshow);
    const filteredSteps = steps.filter((step) => step.id && educationStepIdsAllowlist.has(step.id));
    const filteredStepIds: string[] = filteredSteps.map((step) => step?.id).filter((stepId) => !!stepId) as string[];

    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    function closeTour() {
        setIsOpen(false);
        setReshow(false);
        // add conditional steps where its pre-requisite step ID is in our list of IDs we mark as completed
        const conditionalStepIds = getConditionalStepIdsToAdd(stepIds, filteredStepIds);
        const finalStepIds = [...filteredStepIds, ...conditionalStepIds];
        const convertedIds = finalStepIds.map((id) => convertStepId(id, userUrn || ''));
        const stepStates = convertedIds.map((id) => ({ id, properties: [] }));
        batchUpdateStepStates({ variables: { input: { states: stepStates } } }).then(() => {
            const results = convertedIds.map((id) => ({ id, properties: [{}] }) as StepStateResult);
            setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, ...results] : results));
        });
    }

    const shouldSkipOnboardingTour = useShouldSkipOnboardingTour();

    if (!filteredSteps.length || shouldSkipOnboardingTour) return null;

    return (
        <Tour
            onRequestClose={closeTour}
            steps={filteredSteps}
            isOpen={isOpen}
            scrollOffset={-100}
            rounded={10}
            scrollDuration={500}
            accentColor={accentColor}
            lastStepNextButton={<Button>Let&apos;s go!</Button>}
        />
    );
};
