import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import { buildConfluenceDocumentsIngestionState } from '@app/context/import/buildConfluenceDocumentsIngestionState';
import { buildGitHubDocumentsIngestionState } from '@app/context/import/buildGitHubDocumentsIngestionState';
import { buildNotionDocumentsIngestionState } from '@app/context/import/buildNotionDocumentsIngestionState';
import { useLaunchDocumentIngestionSource } from '@app/context/import/hooks/useLaunchDocumentIngestionSource';
import { ImportSourceType } from '@app/context/import/import.types';

const mockLaunch = vi.fn();

vi.mock('@app/ingestV2/source/multiStepBuilder/hooks/useLaunchIngestionSourceCreate', () => ({
    useLaunchIngestionSourceCreate: () => mockLaunch,
}));

describe('useLaunchDocumentIngestionSource', () => {
    it('launches github, notion, and confluence builders', () => {
        const { result } = renderHook(() => useLaunchDocumentIngestionSource());

        result.current({ source: ImportSourceType.GITHUB });
        expect(mockLaunch).toHaveBeenCalledWith({
            sourceType: 'github-documents',
            initialBuilderState: buildGitHubDocumentsIngestionState(),
        });

        result.current({ source: ImportSourceType.NOTION });
        expect(mockLaunch).toHaveBeenCalledWith({
            sourceType: 'notion',
            initialBuilderState: buildNotionDocumentsIngestionState(),
        });

        result.current({ source: ImportSourceType.CONFLUENCE });
        expect(mockLaunch).toHaveBeenCalledWith({
            sourceType: 'confluence',
            initialBuilderState: buildConfluenceDocumentsIngestionState(),
        });
    });
});
