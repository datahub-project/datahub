import { Button } from '@components';
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

export default function IngestionSourceNavigationButtons() {
    const { submit, cancel, isFinalStep, isCurrentStepCompleted } = useMultiStepContext<
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

    const navigationButtons = useMemo(() => {
        const buttons: React.ReactNode[] = [];

        buttons.push(
            <Button key="cancel" size="sm" variant="text" color="gray" onClick={cancel}>
                Cancel
            </Button>,
        );

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
    }, [cancel, isFinalStep, isCurrentStepCompleted, isSaveAndRunInProgress, onSave, onSaveAndRun]);

    return <ButtonsContainer>{navigationButtons}</ButtonsContainer>;
}
