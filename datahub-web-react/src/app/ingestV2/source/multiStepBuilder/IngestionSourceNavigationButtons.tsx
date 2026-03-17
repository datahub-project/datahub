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

const ButtonWithoutWrapping = styled(Button)`
    white-space: nowrap;
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
            <ButtonWithoutWrapping key="cancel" size="sm" variant="text" color="gray" onClick={cancel}>
                Cancel
            </ButtonWithoutWrapping>,
        );

        if (isFinalStep()) {
            buttons.push(
                <ButtonWithoutWrapping
                    size="sm"
                    variant="secondary"
                    disabled={!isCurrentStepCompleted() || isSaveAndRunInProgress}
                    onClick={onSave}
                    data-testid="save-button"
                >
                    Save
                </ButtonWithoutWrapping>,
                <ButtonWithoutWrapping
                    size="sm"
                    disabled={!isCurrentStepCompleted() || isSaveAndRunInProgress}
                    onClick={onSaveAndRun}
                    data-testid="save-and-run-button"
                >
                    Save and Run
                </ButtonWithoutWrapping>,
            );
        }

        return buttons;
    }, [cancel, isFinalStep, isCurrentStepCompleted, isSaveAndRunInProgress, onSave, onSaveAndRun]);

    return <ButtonsContainer>{navigationButtons}</ButtonsContainer>;
}
