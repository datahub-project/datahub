import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { buildGitHubDocumentsIngestionState } from '@app/context/import/buildGitHubDocumentsIngestionState';
import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import { useLaunchIngestionSourceCreate } from '@app/ingestV2/source/multiStepBuilder/hooks/useLaunchIngestionSourceCreate';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { PageRoutes } from '@conf/Global';

const mockPush = vi.fn();

vi.mock('react-router', () => ({
    useHistory: () => ({ push: mockPush }),
}));

vi.mock('@app/ingestV2/hooks/useIngestionOnboardingRedesignV1', () => ({
    useIngestionOnboardingRedesignV1: vi.fn(),
}));

describe('useLaunchIngestionSourceCreate', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('routes to the create page when onboarding redesign v1 is enabled', () => {
        (useIngestionOnboardingRedesignV1 as unknown as ReturnType<typeof vi.fn>).mockReturnValue(true);
        const initialBuilderState = buildGitHubDocumentsIngestionState();
        const { result } = renderHook(() => useLaunchIngestionSourceCreate());

        result.current({
            sourceType: 'github-documents',
            initialBuilderState,
            initialStepIndex: 1,
        });

        expect(mockPush).toHaveBeenCalledWith(`${PageRoutes.INGESTION_CREATE}?sourceType=github-documents`, {
            initialBuilderState,
            initialStepIndex: 1,
        });
    });

    it('opens the legacy create modal via sources tab deep link when redesign is disabled', () => {
        (useIngestionOnboardingRedesignV1 as unknown as ReturnType<typeof vi.fn>).mockReturnValue(false);
        const initialBuilderState = buildGitHubDocumentsIngestionState();
        const { result } = renderHook(() => useLaunchIngestionSourceCreate());

        result.current({
            sourceType: 'github-documents',
            initialBuilderState,
        });

        expect(mockPush).toHaveBeenCalledWith(tabUrlMap[TabType.Sources], {
            openCreateIngestionModal: true,
            initialBuilderState,
            sourceType: 'github-documents',
        });
    });
});
