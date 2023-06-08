import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { StepStateResult } from '../../types.generated';
import { OnboardingConfig } from './OnboardingConfig';
import { ConditionalStep, OnboardingStep } from './OnboardingStep';

export function convertStepId(stepId: string, userUrn: string) {
    const step = OnboardingConfig.find((configStep) => configStep.id === stepId);
    return `${userUrn}-${step?.id}`;
}

// used to get all of the steps on our initial fetch
export function getStepIds(userUrn: string) {
    return OnboardingConfig.map((step) => `${userUrn}-${step.id}`);
}

const StepTitle = styled(Typography.Title)`
    margin-botton: 5px;
`;

function hasStepBeenSeen(stepId: string, userUrn: string, educationSteps: StepStateResult[]) {
    const convertedStepId = convertStepId(stepId, userUrn);
    return educationSteps.find((step) => step.id === convertedStepId);
}

export function getStepsToRender(
    educationSteps: StepStateResult[] | null,
    stepIds: string[],
    conditionalSteps: ConditionalStep[],
    userUrn: string,
    reshow: boolean,
): OnboardingStep[] {
    if (!educationSteps) return [];
    const filteredStepIds: string[] = reshow
        ? stepIds
        : stepIds.filter((stepId) => !hasStepBeenSeen(stepId, userUrn, educationSteps));

    const finalStepIds = [...filteredStepIds];
    // add conditional steps if they haven't seen the conditional step but have seen the pre-requisite step
    conditionalSteps.forEach((conditionalStep) => {
        if (
            !hasStepBeenSeen(conditionalStep.stepId, userUrn, educationSteps) &&
            hasStepBeenSeen(conditionalStep.preRequisiteStepId, userUrn, educationSteps)
        ) {
            finalStepIds.push(conditionalStep.stepId);
        }
    });

    return finalStepIds
        .map((stepId) => OnboardingConfig.find((step: OnboardingStep) => step.id === stepId))
        .filter((step) => !!step)
        .map((step) => ({
            ...step,
            content: (
                <div>
                    <StepTitle level={5}>{step?.title}</StepTitle>
                    <div>{step?.content}</div>
                </div>
            ),
        }));
}
