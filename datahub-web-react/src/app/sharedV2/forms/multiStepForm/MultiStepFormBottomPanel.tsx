import { Button, Text } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { Step } from '@app/sharedV2/forms/multiStepForm/types';

const Container = styled.div`
    display: flex;
    flex: 1;
    flex-direction: row;
    align-items: center;
`;

const Spacer = styled.div`
    flex: 1;
`;

const ButtonGroup = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    width: 30%;
`;

const LeftButtonGroup = styled(ButtonGroup)`
    justify-content: flex-start;
`;

const RightButtonGroup = styled(ButtonGroup)`
    justify-content: flex-end;
`;

interface Props {
    showSubmitButton?: boolean;
    renderLeftButtons?: (buttons: React.ReactNode[]) => React.ReactNode;
    renderRightButtons?: (buttons: React.ReactNode[]) => React.ReactNode;
}

export function MultiStepFormBottomPanel<TState, TStep extends Step>({
    showSubmitButton,
    renderLeftButtons,
    renderRightButtons,
}: Props) {
    const {
        goToNext,
        canGoToNext,
        canGoToPrevious,
        goToPrevious,
        currentStepIndex,
        totalSteps,
        submit,
        cancel,
        isFinalStep,
        isCurrentStepCompleted,
    } = useMultiStepContext<TState, TStep>();

    const [isSubmitInProgress, setIsSubmitInProgress] = useState<boolean>(false);

    const onSubmit = useCallback(async () => {
        setIsSubmitInProgress(true);
        try {
            await submit?.();
        } finally {
            setIsSubmitInProgress(false);
        }
    }, [submit]);

    const leftButtons = useMemo(() => {
        const buttons: React.ReactNode[] = [];

        if (canGoToPrevious()) {
            buttons.push(
                <Button size="sm" variant="secondary" onClick={goToPrevious}>
                    Back
                </Button>,
            );
        }

        return buttons;
    }, [canGoToPrevious, goToPrevious]);

    const rightButtons = useMemo(() => {
        const buttons: React.ReactNode[] = [];

        buttons.push(
            <Button size="sm" variant="text" color="gray" onClick={cancel}>
                Cancel
            </Button>,
        );

        if (canGoToNext()) {
            buttons.push(
                <Button size="sm" disabled={!isCurrentStepCompleted()} onClick={goToNext}>
                    Next
                </Button>,
            );
        }

        if (showSubmitButton && isFinalStep()) {
            buttons.push(
                <Button size="sm" disabled={!isCurrentStepCompleted() || isSubmitInProgress} onClick={onSubmit}>
                    Submit
                </Button>,
            );
        }

        return buttons;
    }, [canGoToNext, isFinalStep, cancel, isCurrentStepCompleted, goToNext, onSubmit, showSubmitButton]);

    return (
        <Container>
            <LeftButtonGroup>{renderLeftButtons ? renderLeftButtons(leftButtons) : leftButtons}</LeftButtonGroup>
            <Spacer />

            <Text>
                {currentStepIndex + 1} / {totalSteps}
            </Text>
            <Spacer />

            <RightButtonGroup>{renderRightButtons ? renderRightButtons(rightButtons) : rightButtons}</RightButtonGroup>
        </Container>
    );
}
