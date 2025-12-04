import { Button } from '@components';
import { useCallback, useState } from 'react';

import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

import { IngestionSourceFormStep, MultiStepSourceBuilderState, SubmitOptions } from './types';

export function IngestionSourceBottomPanel() {
    const { isFinalStep, isCurrentStepCompleted, submit } = useMultiStepContext<
        MultiStepSourceBuilderState,
        IngestionSourceFormStep,
        SubmitOptions
    >();
    const [isSaveAndRunInProgress, setIsSaveAndRunInPropgress] = useState<boolean>(false);

    const save = useCallback(
        async (options: SubmitOptions) => {
            setIsSaveAndRunInPropgress(true);
            try {
                await submit?.(options);
            } finally {
                setIsSaveAndRunInPropgress(false);
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
                >
                    Save
                </Button>,
                <Button size="sm" disabled={!isCurrentStepCompleted() || isSaveAndRunInProgress} onClick={onSaveAndRun}>
                    Save and Run
                </Button>,
            ];
        },
        [isFinalStep, isCurrentStepCompleted, onSaveAndRun, isSaveAndRunInProgress],
    );

    return <MultiStepFormBottomPanel renderRightButtons={renderRightButtons} />;
}
