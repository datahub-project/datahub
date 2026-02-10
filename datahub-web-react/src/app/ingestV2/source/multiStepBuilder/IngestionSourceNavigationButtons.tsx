import { Button, Tooltip } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import {
    IngestionSourceFormStep,
    MultiStepSourceBuilderState,
    SubmitOptions,
} from '@app/ingestV2/source/multiStepBuilder/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

const ButtonsContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;
interface Props {
    disabledNextTooltip?: string;
}

export default function IngestionSourceNavigationButtons({ disabledNextTooltip }: Props) {
    const { goToNext, canGoToNext, submit, cancel, isFinalStep, isCurrentStepCompleted, currentStepIndex } =
        useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep, SubmitOptions>();

    const [isSaveAndRunInProgress, setIsSaveAndRunInProgress] = useState<boolean>(false);

    const save = useCallback(
        async (options: SubmitOptions) => {
            setIsSaveAndRunInProgress(true);
            try {
                await submit?.(options);
            } finally {
                setIsSaveAndRunInProgress(false);
            }
        },
        [submit],
    );

    const onSave = useCallback(async () => {
        await save({ shouldRun: false });
    }, [save]);

    const onSaveAndRun = useCallback(async () => {
        await save({ shouldRun: true });
    }, [save]);

    const navigationButtons = useMemo(() => {
        const buttons: React.ReactNode[] = [];

        buttons.push(
            <Button key="cancel" size="sm" variant="text" color="gray" onClick={cancel}>
                Cancel
            </Button>,
        );

        if (canGoToNext() && currentStepIndex !== 0) {
            const isDisabled = !isCurrentStepCompleted();
            const nextButton = (
                <Button key="next" size="sm" disabled={isDisabled} onClick={goToNext} data-testid="next-button">
                    Next
                </Button>
            );

            buttons.push(
                isDisabled ? (
                    <Tooltip
                        key="next"
                        title={
                            disabledNextTooltip || 'Please complete all required fields before moving to the next step'
                        }
                    >
                        <span>{nextButton}</span>
                    </Tooltip>
                ) : (
                    nextButton
                ),
            );
        }

        if (isFinalStep()) {
            buttons.push(
                <Button
                    size="sm"
                    variant="outline"
                    disabled={!isCurrentStepCompleted() || isSaveAndRunInProgress}
                    onClick={onSave}
                >
                    Save
                </Button>,
                <Button size="sm" disabled={!isCurrentStepCompleted() || isSaveAndRunInProgress} onClick={onSaveAndRun}>
                    Save and Run
                </Button>,
            );
        }

        return buttons;
    }, [
        cancel,
        canGoToNext,
        currentStepIndex,
        isFinalStep,
        isCurrentStepCompleted,
        goToNext,
        disabledNextTooltip,
        isSaveAndRunInProgress,
        onSave,
        onSaveAndRun,
    ]);

    return <ButtonsContainer>{navigationButtons}</ButtonsContainer>;
}
