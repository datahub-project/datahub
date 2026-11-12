import { beforeEach, describe, expect, it, vi } from 'vitest';

import { buildGitHubDocumentsIngestionState } from '@app/context/import/buildGitHubDocumentsIngestionState';
import { useLaunchIngestionSourceCreate } from '@app/ingestV2/source/multiStepBuilder/hooks/useLaunchIngestionSourceCreate';
import { PageRoutes } from '@conf/Global';
import { renderHook } from '@utils/test-utils/renderHook';

const mockPush = vi.fn();

vi.mock('react-router', () => ({
    useHistory: () => ({ push: mockPush }),
    useLocation: () => ({ pathname: '/context/documents', search: '?tab=all' }),
}));

describe('useLaunchIngestionSourceCreate', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('routes to the create page with the pre-filled builder state', () => {
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
            backUrl: '/context/documents?tab=all',
        });
    });
});
