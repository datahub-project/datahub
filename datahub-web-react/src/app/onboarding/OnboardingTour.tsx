import { Button } from 'antd';
import React, { useContext, useEffect } from 'react';
import Tour from 'reactour';
import { useBatchUpdateStepStatesMutation } from '../../graphql/step.generated';
import { EducationStepsContext } from '../../providers/EducationStepsContext';
import { StepStateResult } from '../../types.generated';
import { useUserContext } from '../context/useUserContext';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useIsThemeV2 } from '../useIsThemeV2';
import { convertStepId, getConditionalStepIdsToAdd, getStepsToRender } from './utils';
import useShouldSkipOnboardingTour from './useShouldSkipOnboardingTour';
import OnboardingContext from './OnboardingContext';

type Props = {
    stepIds: string[];
};

export const OnboardingTour = ({ stepIds }: Props) => {
    const { educationSteps, setEducationSteps, educationStepIdsAllowlist } = useContext(EducationStepsContext);
    const { isTourOpen, setIsTourOpen, tourReshow, setTourReshow } = useContext(OnboardingContext);
    const userUrn = useUserContext()?.user?.urn;
    const isThemeV2 = useIsThemeV2();
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

        return () => document.removeEventListener('keydown', handleKeyDown);
    }, [setIsTourOpen, setTourReshow]);

    const steps = getStepsToRender(educationSteps, stepIds, userUrn || '', tourReshow);
    const filteredSteps = steps.filter((step) => step.id && educationStepIdsAllowlist.has(step.id));
    const filteredStepIds: string[] = filteredSteps.map((step) => step?.id).filter((stepId) => !!stepId) as string[];

    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    function closeTour() {
        setIsTourOpen(false);
        setTourReshow(false);
        // add conditional steps where its pre-requisite step ID is in our list of IDs we mark as completed
        const conditionalStepIds = getConditionalStepIdsToAdd(stepIds, filteredStepIds);
        const finalStepIds = [...filteredStepIds, ...conditionalStepIds];
        const convertedIds = finalStepIds.map((id) => convertStepId(id, userUrn || ''));
        const stepStates = convertedIds.map((id) => ({ id, properties: [] }));
        batchUpdateStepStates({ variables: { input: { states: stepStates } } }).then(() => {
            const results = convertedIds.map((id) => ({ id, properties: [{}] } as StepStateResult));
            setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, ...results] : results));
        });
    }

    const shouldSkipOnboardingTour = useShouldSkipOnboardingTour();

    if (!filteredSteps.length || shouldSkipOnboardingTour) return null;

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
