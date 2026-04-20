import { Button } from 'antd';
import React, { useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation } from 'react-router-dom';
import Tour from 'reactour';
import { useTheme } from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import useShouldSkipOnboardingTour from '@app/onboarding/useShouldSkipOnboardingTour';
import { convertStepId, getConditionalStepIdsToAdd, getStepsToRender } from '@app/onboarding/utils';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';
import { StepStateResult } from '@types';

type Props = {
    stepIds: string[];
};

export const OnboardingTour = ({ stepIds }: Props) => {
    const { educationSteps, setEducationSteps, educationStepIdsAllowlist } = useContext(EducationStepsContext);
    const userUrn = useUserContext()?.user?.urn;
    const theme = useTheme();
    const { isTourOpen, tourReshow, setTourReshow, setIsTourOpen, setIsOnboardingAvailable } =
        useContext(OnboardingContext);
    const location = useLocation();
    const accentColor = theme.colors.bgSurfaceBrand;

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

    const prevStepRef = useRef<number | null>(null);
    const [updateKey, setUpdateKey] = useState(0);

    const handleStepChange = (currStep: number) => {
        if (prevStepRef.current !== currStep) {
            const step = filteredSteps[currStep];
            if (step && step.tabName) {
                // Force Reactour to recalculate highlight after action scrolls the tab
                setUpdateKey((prev) => prev + 1);
            }
            prevStepRef.current = currStep;
        }
    };

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

    const canTourBeShown = useMemo(() => {
        // Do not show tour for home page (see `WelcomeToDataHubModal`)
        if (isHomepage) return false;

        return true;
    }, [isHomepage]);

    const canTourBeReshown = useMemo(() => {
        // should have `filteredSteps` when `tourReshow` is true
        // see `getStepsToRender` for details
        if (!educationSteps) return false;
        return canTourBeShown && stepIds.length > 0;
    }, [canTourBeShown, educationSteps, stepIds]);

    // Register and unregister the tour availability in the context
    // FYI: it's using to hide the tour button when tour is not available
    useEffect(() => {
        setIsOnboardingAvailable(canTourBeReshown);
        return () => setIsOnboardingAvailable(false);
    }, [canTourBeReshown, setIsOnboardingAvailable]);

    // For automatic tours (tourReshow=false), only check if we have steps to show and not on homepage
    // For manual tours (tourReshow=true), also check the global skip flag
    if (!canTourBeShown || !filteredSteps.length || (tourReshow && shouldSkipOnboardingTour)) {
        return null;
    }

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
            getCurrentStep={handleStepChange}
            update={updateKey}
        />
    );
};
