import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import LifecycleStageModal from '@app/entityV2/shared/EntityDropdown/LifecycleStageModal';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityType } from '@src/types.generated';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const TERM_URN = 'urn:li:glossaryTerm:my_term';
const DRAFT = 'urn:li:lifecycleStageType:DRAFT';
const PUBLISHED = 'urn:li:lifecycleStageType:PUBLISHED';

const setStage = vi.fn();
let stages: any[] = [];
let stagesError: Error | undefined;

vi.mock('@graphql/lifecycle.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/lifecycle.generated')>()),
    useListLifecycleStagesQuery: () => ({
        data: stagesError ? undefined : { listLifecycleStages: stages },
        loading: false,
        error: stagesError,
    }),
    useSetLifecycleStageMutation: () => [setStage],
}));

function renderModal() {
    const onClose = vi.fn();
    const onSuccess = vi.fn();
    render(
        <CustomThemeProvider>
            <LifecycleStageModal
                urn={TERM_URN}
                entityType={EntityType.GlossaryTerm}
                currentStage={{ urn: DRAFT, name: 'Draft' }}
                onClose={onClose}
                onSuccess={onSuccess}
            />
        </CustomThemeProvider>,
    );
    return { onClose, onSuccess };
}

function pickPublished() {
    fireEvent.click(screen.getByTestId('lifecycle-stage-select-base'));
    fireEvent.click(screen.getByTestId(`option-${PUBLISHED}`));
}

beforeEach(() => {
    vi.clearAllMocks();
    mockVisibilityObserver();
    stagesError = undefined;
    stages = [
        { urn: DRAFT, name: 'Draft', userAssignable: true, allowedPreviousStages: null },
        { urn: PUBLISHED, name: 'Published', userAssignable: true, allowedPreviousStages: [DRAFT] },
    ];
});

describe('LifecycleStageModal', () => {
    it('sets the stage', async () => {
        setStage.mockResolvedValue({ data: { setLifecycleStage: true } });
        const { onClose, onSuccess } = renderModal();

        pickPublished();
        fireEvent.click(screen.getByTestId('lifecycle-stage-submit'));

        await waitFor(() =>
            expect(setStage).toHaveBeenCalledWith(
                expect.objectContaining({ variables: { urn: TERM_URN, lifecycleStageUrn: PUBLISHED } }),
            ),
        );
        expect(onSuccess).toHaveBeenCalled();
        expect(onClose).toHaveBeenCalled();
    });

    it('shows the server error and stays open when the change is rejected', async () => {
        setStage.mockRejectedValue(new Error('Moving to lifecycle stage PUBLISHED is not allowed.'));
        const { onClose } = renderModal();

        pickPublished();
        fireEvent.click(screen.getByTestId('lifecycle-stage-submit'));

        expect(await screen.findByText(/is not allowed/)).toBeInTheDocument();
        expect(onClose).not.toHaveBeenCalled();
    });

    it('explains when no stage can be reached and keeps submit disabled', () => {
        stages = [{ urn: DRAFT, name: 'Draft', userAssignable: true, allowedPreviousStages: null }];
        renderModal();

        expect(screen.getByText('No other stage can be reached from the current one.')).toBeInTheDocument();
        expect(screen.getByTestId('lifecycle-stage-submit')).toBeDisabled();
    });

    it('reports a failed stage lookup instead of claiming no stage is reachable', () => {
        stagesError = new Error('network down');
        renderModal();

        expect(screen.getByText('Could not load the lifecycle stages. Try again later.')).toBeInTheDocument();
        expect(screen.queryByText('No other stage can be reached from the current one.')).toBeNull();
        expect(screen.getByTestId('lifecycle-stage-submit')).toBeDisabled();
    });
});
