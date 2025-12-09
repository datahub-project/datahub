import { Button, Icon } from '@components';
import React from 'react';

import {
    DismissButton,
    TaskActions,
    TaskContent,
    TaskDescription,
    TaskIconWrapper,
    TaskItem,
    TaskTitle,
} from '@app/homeV3/freeTrial/FreeTrialContent.styles';
import { OnboardingStep } from '@app/onboarding/types';

interface TaskItemComponentProps {
    step: OnboardingStep;
    isCompleted: boolean;
    onDismiss: (id: string) => void;
    onStart: (id: string) => void;
}

/**
 * Component to render a task item in the free trial content
 */
export const TaskItemComponent = ({ step, isCompleted, onDismiss, onStart }: TaskItemComponentProps) => {
    return (
        <TaskItem $isCompleted={isCompleted}>
            <TaskIconWrapper>
                <Icon icon={step.icon || 'Star'} color="violet" size="xl" source="phosphor" />
            </TaskIconWrapper>
            <TaskContent>
                <TaskTitle>{step.title}</TaskTitle>
                <TaskDescription>{step.content}</TaskDescription>
            </TaskContent>
            <TaskActions>
                <DismissButton onClick={() => onDismiss(step.id || '')}>Dismiss</DismissButton>
                {!isCompleted && (
                    <Button size="sm" variant="outline" onClick={() => onStart(step.id || '')}>
                        Start
                    </Button>
                )}
            </TaskActions>
        </TaskItem>
    );
};
