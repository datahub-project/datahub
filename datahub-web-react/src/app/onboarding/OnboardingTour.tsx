import { Button } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import Tour from 'reactour';
import { useBatchUpdateStepStatesMutation } from '../../graphql/step.generated';
import { EducationStepsContext } from '../../providers/EducationStepsContext';
import { StepStateResult } from '../../types.generated';
import { useUserContext } from '../context/useUserContext';
import { convertStepId, getStepsToRender } from './utils';
import { ConditionalStep } from './OnboardingStep';

type Props = {
    stepIds: string[];
    conditionalSteps?: ConditionalStep[];
};

export const OnboardingTour = ({ stepIds, conditionalSteps = [] }: Props) => {
    const { educationSteps, setEducationSteps, educationStepIdsAllowlist } = useContext(EducationStepsContext);
    const userUrn = useUserContext()?.user?.urn;
    const [isOpen, setIsOpen] = useState(true);
    const [reshow, setReshow] = useState(false);
    const accentColor = '#5cb7b7';

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

    const steps = getStepsToRender(educationSteps, stepIds, conditionalSteps, userUrn || '', reshow);
    const filteredSteps = steps.filter((step) => step.id && educationStepIdsAllowlist.has(step.id));
    const filteredStepIds: string[] = filteredSteps.map((step) => step?.id).filter((stepId) => !!stepId) as string[];

    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    function closeTour() {
        setIsOpen(false);
        setReshow(false);
        const finalStepIds = [...filteredStepIds];
        // mark conditional step as seen if we're seeing its preRequisite step right now
        conditionalSteps.forEach((conditionalStep) => {
            if (filteredStepIds.includes(conditionalStep.preRequisiteStepId)) {
                finalStepIds.push(conditionalStep.stepId);
            }
        });
        const convertedIds = finalStepIds.map((id) => convertStepId(id, userUrn || ''));
        const stepStates = convertedIds.map((id) => ({ id, properties: [] }));
        batchUpdateStepStates({ variables: { input: { states: stepStates } } }).then(() => {
            const results = convertedIds.map((id) => ({ id, properties: [{}] } as StepStateResult));
            setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, ...results] : results));
        });
    }

    if (!filteredSteps.length) return null;

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
