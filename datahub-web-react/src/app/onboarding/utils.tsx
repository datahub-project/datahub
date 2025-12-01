import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { OnboardingConfig } from '@app/onboarding/OnboardingConfig';
import { OnboardingStep } from '@app/onboarding/types';

import { StepStateResult } from '@types';
import { getFreeTrialOnboardingIds } from './configV2/FreeTrialConfig';

export function convertStepId(stepId: string, userUrn: string) {
    const step = OnboardingConfig.find((configStep) => configStep.id === stepId);
    return `${userUrn}-${step?.id}`;
}

// used to get all of the steps on our initial fetch
export function getStepIds(userUrn: string) {
    return getUserSpecificStepIds(userUrn).concat(getInstanceLevelOnboardingStepIds());
}

/**
 * Get all of the steps that are specific to the user
 */
export function getUserSpecificStepIds(userUrn: string) {
    return OnboardingConfig.map((step) => `${userUrn}-${step.id}`);
}

/**
 * Get all of the steps that are on the instance level (not user specific)
 */
export function getInstanceLevelOnboardingStepIds() {
    return getFreeTrialOnboardingIds();
}

// if the user just saw the prerequisiteStepId (in stepIdsToAdd) of a conditional step, we need to add this conditional step
export function getConditionalStepIdsToAdd(providedStepIds: string[], stepIdsToAdd: string[]) {
    const conditionalStepIds: string[] = [];

    const providedSteps = providedStepIds
        .map((stepId) => OnboardingConfig.find((step: OnboardingStep) => step.id === stepId))
        .filter((step) => !!step);

    providedSteps.forEach((step) => {
        if (
            step?.prerequisiteStepId &&
            stepIdsToAdd.includes(step?.prerequisiteStepId) &&
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

export function hasSeenPrerequisiteStepIfExists(
    step: OnboardingStep,
    userUrn: string,
    educationSteps: StepStateResult[],
) {
    if (step?.prerequisiteStepId) {
        if (hasStepBeenSeen(step.prerequisiteStepId, userUrn, educationSteps)) {
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
        .filter((step) => hasSeenPrerequisiteStepIfExists(step as OnboardingStep, userUrn, educationSteps)) // if this is a conditional step, only keep it if the prerequisite step has been seen
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

// filter out action steps from the initial steps that should be shown
export function getInitialAllowListIds() {
    return OnboardingConfig.filter((config) => !config.isActionStep).map((config) => config.id as string);
}

/**
 * Helper to get a step's property value by key from educationSteps
 */
export const getStepPropertyByKey = (
    educationSteps: StepStateResult[] | null,
    stepId: string,
    propKey: string,
): string | null => {
    if (!educationSteps) return null;
    const stepResult = educationSteps.find((step) => step.id === stepId);
    if (!stepResult) return null;
    const entry = stepResult.properties.find((prop) => prop.key === propKey);
    return entry?.value || null;
};
