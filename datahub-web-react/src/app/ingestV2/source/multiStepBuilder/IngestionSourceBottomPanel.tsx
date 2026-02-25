import { Button } from '@components';
import React, { useCallback, useState } from 'react';

import {
    IngestionSourceFormStep,
    MultiStepSourceBuilderState,
    SubmitOptions,
} from '@app/ingestV2/source/multiStepBuilder/types';
import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function IngestionSourceBottomPanel() {
    const { isFinalStep, isCurrentStepCompleted, submit } = useMultiStepContext<
        MultiStepSourceBuilderState,
        IngestionSourceFormStep,
        SubmitOptions
    >();
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

    const renderRightButtons = useCallback(
        (buttons: React.ReactNode[]) => {
            if (!isFinalStep()) return buttons;
            return [
                ...buttons,
                <Button
                    size="sm"
                    variant="outline"
                    disabled={!isCurrentStepCompleted() || isSaveAndRunInProgress}
                    onClick={onSave}
                    data-testid="save-button"
                >
                    Save
                </Button>,
                <Button
                    size="sm"
                    disabled={!isCurrentStepCompleted() || isSaveAndRunInProgress}
                    onClick={onSaveAndRun}
                    data-testid="save-and-run-button"
                >
                    Save and Run
                </Button>,
            ];
        },
        [isFinalStep, isCurrentStepCompleted, onSave, onSaveAndRun, isSaveAndRunInProgress],
    );

    return (
        <MultiStepFormBottomPanel
            renderRightButtons={renderRightButtons}
            disabledNextTooltip="Enter a name to continue"
        />
    );
}
