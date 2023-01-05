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
        : stepIds.filter((stepId) => {
              const convertedStepId = convertStepId(stepId, userUrn);
              // if we don't have this step in our educationSteps from GMS we haven't seen it yet
              return !educationSteps.find((step) => step.id === convertedStepId);
          });

    return filteredStepIds
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
