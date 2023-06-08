import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { StepStateResult } from '../../types.generated';
import { OnboardingConfig } from './OnboardingConfig';
import { OnboardingStep } from './OnboardingStep';

export function convertStepId(stepId: string, userUrn: string) {
    const step = OnboardingConfig.find((configStep) => configStep.id === stepId);
    return `${userUrn}-${step?.id}`;
}

// used to get all of the steps on our initial fetch
export function getStepIds(userUrn: string) {
    return OnboardingConfig.map((step) => `${userUrn}-${step.id}`);
}

// if the user just saw the preRequisiteStepId (in stepIdsToAdd) of a conditional step, we need to add this conditional step
export function getConditionalStepIdsToAdd(providedStepIds: string[], stepIdsToAdd: string[]) {
    const conditionalStepIds: string[] = [];

    const providedSteps = providedStepIds
        .map((stepId) => OnboardingConfig.find((step: OnboardingStep) => step.id === stepId))
        .filter((step) => !!step);

    providedSteps.forEach((step) => {
        if (
            step?.preRequisiteStepId &&
            stepIdsToAdd.includes(step?.preRequisiteStepId) &&
            !stepIdsToAdd.includes(step.id || '')
        ) {
            conditionalStepIds.push(step.id || '');
        }
    });

    return conditionalStepIds;
}

function hasStepBeenSeen(stepId: string, userUrn: string, educationSteps: StepStateResult[]) {
    const convertedStepId = convertStepId(stepId, userUrn);
    return educationSteps.some((step) => step.id === convertedStepId);
}

// add conditional steps if they have seen the pre-requisite step only
export function filterConditionalStep(step: OnboardingStep, userUrn: string, educationSteps: StepStateResult[]) {
    if (step?.preRequisiteStepId) {
        if (hasStepBeenSeen(step.preRequisiteStepId, userUrn, educationSteps)) {
            return true;
        }
        return false;
    }
    return true;
}

const StepTitle = styled(Typography.Title)`
    margin-botton: 5px;
`;

export function getStepsToRender(
    educationSteps: StepStateResult[] | null,
    stepIds: string[],
    userUrn: string,
    reshow: boolean,
): OnboardingStep[] {
    if (!educationSteps) return [];
    const filteredStepIds: string[] = reshow
        ? stepIds
        : stepIds.filter((stepId) => !hasStepBeenSeen(stepId, userUrn, educationSteps));

    const finalStepIds = [...filteredStepIds];

    return finalStepIds
        .map((stepId) => OnboardingConfig.find((step: OnboardingStep) => step.id === stepId))
        .filter((step) => !!step)
        .filter((step) => filterConditionalStep(step as OnboardingStep, userUrn, educationSteps))
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
