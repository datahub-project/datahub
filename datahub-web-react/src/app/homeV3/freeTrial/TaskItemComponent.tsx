import { Icon } from '@components';
import React from 'react';

import {
    TaskCheckbox,
    TaskContent,
    TaskDescription,
    TaskIconWrapper,
    TaskItem,
    TaskTitleClickable,
} from '@app/homeV3/freeTrial/FreeTrialOnboardingContent.styles';
import { OnboardingStep } from '@app/onboarding/types';

interface TaskItemComponentProps {
    step: OnboardingStep;
    isComplete: boolean;
    onStart: (id: string) => void;
}

/**
 * Component to render a task item in the free trial content
 */
export const TaskItemComponent = ({ step, isComplete, onStart }: TaskItemComponentProps) => {
    return (
        <TaskItem>
            <TaskIconWrapper>
                <Icon icon={step.icon || 'Star'} color="violet" size="xl" source="phosphor" />
            </TaskIconWrapper>
            <TaskContent>
                <TaskTitleClickable onClick={() => onStart(step.id || '')} $isComplete={isComplete}>
                    {step.title}
                </TaskTitleClickable>
                <TaskDescription>{step.content}</TaskDescription>
            </TaskContent>
            <TaskCheckbox $isComplete={isComplete}>
                {isComplete && <Icon icon="Check" color="white" size="md" source="phosphor" weight="bold" />}
            </TaskCheckbox>
        </TaskItem>
    );
};
