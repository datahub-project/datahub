import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useIsDocumentationFileUploadV1Enabled } from '@app/shared/hooks/useIsDocumentationFileUploadV1Enabled';
import { useAppConfig } from '@app/useAppConfig';

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

describe('useIsDocumentationFileUploadV1Enabled', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return value of documentationFileUploadV1 feature flag', () => {
        (useAppConfig as any).mockReturnValue({
            config: {
                featureFlags: {
                    documentationFileUploadV1: true,
                },
            },
        });

        const { result } = renderHook(() => useIsDocumentationFileUploadV1Enabled());
        expect(result.current).toBe(true);
    });
});
