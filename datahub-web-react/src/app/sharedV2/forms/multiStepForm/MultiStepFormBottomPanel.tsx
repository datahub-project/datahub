import { Button, Text, Tooltip } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
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
    disabledNextTooltip?: string;
}

export function MultiStepFormBottomPanel<TState, TStep extends Step>({
    showSubmitButton,
    renderLeftButtons,
    renderRightButtons,
    disabledNextTooltip,
}: Props) {
    const { t } = useTranslation('shared.misc');
    const { t: tc } = useTranslation('common.actions');
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
                <Button key="back" size="sm" variant="secondary" onClick={goToPrevious} data-testid="back-button">
                    {tc('back')}
                </Button>,
            );
        }

        return buttons;
    }, [canGoToPrevious, goToPrevious, tc]);

    const rightButtons = useMemo(() => {
        const buttons: React.ReactNode[] = [];

        buttons.push(
            <Button key="cancel" size="sm" variant="text" color="gray" onClick={cancel} data-testid="cancel-button">
                {tc('cancel')}
            </Button>,
        );

        if (canGoToNext()) {
            const isDisabled = !isCurrentStepCompleted();
            const nextButton = (
                <Button key="next" size="sm" disabled={isDisabled} onClick={goToNext} data-testid="next-button">
                    {tc('next')}
                </Button>
            );

            buttons.push(
                isDisabled ? (
                    <Tooltip key="next" title={disabledNextTooltip || t('multiStepForm.disabledNextTooltip')}>
                        <span>{nextButton}</span>
                    </Tooltip>
                ) : (
                    nextButton
                ),
            );
        }

        if (showSubmitButton && isFinalStep()) {
            buttons.push(
                <Button
                    key="submit"
                    size="sm"
                    disabled={!isCurrentStepCompleted() || isSubmitInProgress}
                    onClick={onSubmit}
                    data-testid="submit-button"
                >
                    {tc('submit')}
                </Button>,
            );
        }

        return buttons;
    }, [
        disabledNextTooltip,
        canGoToNext,
        isFinalStep,
        cancel,
        isCurrentStepCompleted,
        goToNext,
        onSubmit,
        showSubmitButton,
        isSubmitInProgress,
        t,
        tc,
    ]);

    return (
        <Container>
            <LeftButtonGroup>{renderLeftButtons ? renderLeftButtons(leftButtons) : leftButtons}</LeftButtonGroup>
            <Spacer />

            {/* eslint-disable i18next/no-literal-string -- (untranslated-text) numeric/punctuation separator */}
            <Text data-testid="step-counter">
                {currentStepIndex + 1} / {totalSteps}
            </Text>
            {/* eslint-enable i18next/no-literal-string */}
            <Spacer />

            <RightButtonGroup>{renderRightButtons ? renderRightButtons(rightButtons) : rightButtons}</RightButtonGroup>
        </Container>
    );
}
