import { Button } from 'antd';
import React, { useContext, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import Tour from 'reactour';

import { useUserContext } from '@app/context/useUserContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import OnboardingContext from '@app/onboarding/OnboardingContext';
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
    const isThemeV2 = useIsThemeV2();
    const { isTourOpen, tourReshow, setTourReshow, setIsTourOpen } = useContext(OnboardingContext);
    const location = useLocation();
    const accentColor = isThemeV2 ? REDESIGN_COLORS.BACKGROUND_PURPLE : '#5cb7b7';

    useEffect(() => {
        function handleKeyDown(e) {
            // Allow reshow if Cmnd + Ctrl + T is pressed
            if (e.metaKey && e.ctrlKey && e.key === 't') {
                setTourReshow(true);
                setIsTourOpen(true);
            }
            if (e.metaKey && e.ctrlKey && e.key === 'h') {
                setTourReshow(false);
                setIsTourOpen(false);
            }
        }
        document.addEventListener('keydown', handleKeyDown);
    }, [setTourReshow, setIsTourOpen]);

    // Don't show OnboardingTour on homepage - WelcomeToDataHubModal is used there instead
    const isHomepage = location.pathname === '/';

    const steps = getStepsToRender(educationSteps, stepIds, userUrn || '', tourReshow);
    const filteredSteps = steps.filter((step) => step.id && educationStepIdsAllowlist.has(step.id));
    const filteredStepIds: string[] = filteredSteps.map((step) => step?.id).filter((stepId) => !!stepId) as string[];

    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();
    const shouldSkipOnboardingTour = useShouldSkipOnboardingTour();

    // Automatically open tour for first-time visits when there are unseen steps
    useEffect(() => {
        if (
            !tourReshow && // Only for automatic tours, not manual reshows
            !shouldSkipOnboardingTour && // Don't show if globally disabled
            !isHomepage && // Don't show on homepage - WelcomeToDataHubModal is used there
            filteredSteps.length > 0 && // Only if there are steps to show
            !isTourOpen // Don't open if already open
        ) {
            setIsTourOpen(true);
        }
    }, [filteredSteps.length, tourReshow, shouldSkipOnboardingTour, isHomepage, isTourOpen, setIsTourOpen]);

    function closeTour() {
        setIsTourOpen(false);
        setTourReshow(false);
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

    // For automatic tours (tourReshow=false), only check if we have steps to show and not on homepage
    // For manual tours (tourReshow=true), also check the global skip flag
    if (!filteredSteps.length || isHomepage || (tourReshow && shouldSkipOnboardingTour)) return null;

    return (
        <Tour
            onRequestClose={closeTour}
            steps={filteredSteps}
            isOpen={isTourOpen}
            scrollOffset={-100}
            rounded={10}
            scrollDuration={500}
            accentColor={accentColor}
            lastStepNextButton={<Button>Let&apos;s go!</Button>}
        />
    );
};
