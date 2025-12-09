import { Button, Text } from '@components';
import React, { useCallback, useState } from 'react';
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

export function MultiStepFormBottomPanel<TState, TStep extends Step>() {
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

    return (
        <Container>
            <LeftButtonGroup>
                {canGoToPrevious() ? (
                    <Button size="sm" variant="secondary" onClick={goToPrevious}>
                        Back
                    </Button>
                ) : null}
            </LeftButtonGroup>
            <Spacer />

            <Text>
                {currentStepIndex + 1} / {totalSteps}
            </Text>
            <Spacer />

            <RightButtonGroup>
                <Button size="sm" variant="text" color="gray" onClick={cancel}>
                    Cancel
                </Button>
                {canGoToNext() ? (
                    <Button size="sm" disabled={!isCurrentStepCompleted()} onClick={goToNext}>
                        Next
                    </Button>
                ) : null}
                {isFinalStep() ? (
                    <Button size="sm" disabled={!isCurrentStepCompleted() || isSubmitInProgress} onClick={onSubmit}>
                        Submit
                    </Button>
                ) : null}
            </RightButtonGroup>
        </Container>
    );
}
