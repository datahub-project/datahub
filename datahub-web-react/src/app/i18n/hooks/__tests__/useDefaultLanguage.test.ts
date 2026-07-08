import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DEFAULT_LANGUAGE } from '@app/i18n/constants';
import { useDefaultLanguage } from '@app/i18n/hooks/useDefaultLanguage';
import { useAppConfig } from '@app/useAppConfig';

vi.mock('@app/useAppConfig');

const mockUseAppConfig = vi.mocked(useAppConfig);

function mockConfiguredLocale(i18nDefaultLocale?: string) {
    mockUseAppConfig.mockReturnValue({
        config: { featureFlags: { i18nDefaultLocale } },
    } as unknown as ReturnType<typeof useAppConfig>);
}

describe('useDefaultLanguage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('returns the configured locale when it is a supported language', () => {
        mockConfiguredLocale('de');
        const { result } = renderHook(() => useDefaultLanguage());
        expect(result.current).toBe('de');
    });

    it('falls back to English when the configured locale is not supported', () => {
        mockConfiguredLocale('xx');
        const { result } = renderHook(() => useDefaultLanguage());
        expect(result.current).toBe(DEFAULT_LANGUAGE);
    });

    it('falls back to English when no locale is configured', () => {
        mockConfiguredLocale(undefined);
        const { result } = renderHook(() => useDefaultLanguage());
        expect(result.current).toBe(DEFAULT_LANGUAGE);
    });
});
