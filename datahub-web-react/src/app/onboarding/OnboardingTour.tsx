import { Button } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import Tour from 'reactour';
import { useBatchUpdateStepStatesMutation } from '../../graphql/step.generated';
import { EducationStepsContext } from '../../providers/EducationStepsContext';
import { StepStateResult } from '../../types.generated';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { convertStepId, getStepsToRender } from './utils';

type Props = {
    stepIds: string[];
};

export const OnboardingTour = ({ stepIds }: Props) => {
    const { educationSteps, setEducationSteps, educationStepIdsAllowlist } = useContext(EducationStepsContext);
    const userUrn = useGetAuthenticatedUser()?.corpUser.urn;
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

    const steps = getStepsToRender(educationSteps, stepIds, userUrn || '', reshow);
    const filteredSteps = steps.filter((step) => step.id && educationStepIdsAllowlist.has(step.id));
    const filteredStepIds: string[] = filteredSteps.map((step) => step?.id).filter((stepId) => !!stepId) as string[];

    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    function closeTour() {
        setIsOpen(false);
        setReshow(false);
        const convertedIds = filteredStepIds.map((id) => convertStepId(id, userUrn || ''));
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
