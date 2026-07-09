import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { loadAntdLocale } from '@app/i18n/antdLocale';
import I18nProvider from '@app/i18n/context/I18nProvider';
import { useEffectiveLanguage } from '@app/i18n/hooks/useEffectiveLanguage';
import { useLanguageSync } from '@app/i18n/hooks/useLanguageSync';

vi.mock('@app/i18n/hooks/useLanguageSync');
vi.mock('@app/i18n/hooks/useEffectiveLanguage');
vi.mock('@app/i18n/antdLocale');

const mockAntdLocale = { locale: 'de' } as any;

describe('I18nProvider', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(useLanguageSync).mockReturnValue(undefined);
        vi.mocked(useEffectiveLanguage).mockReturnValue('de');
        vi.mocked(loadAntdLocale).mockResolvedValue(mockAntdLocale);
    });

    it('renders children', () => {
        render(
            <I18nProvider>
                <span>child content</span>
            </I18nProvider>,
        );

        expect(screen.getByText('child content')).toBeTruthy();
    });

    it('calls useLanguageSync for side effects', () => {
        render(
            <I18nProvider>
                <div />
            </I18nProvider>,
        );

        expect(useLanguageSync).toHaveBeenCalled();
    });

    it('loads the antd locale for the effective language', async () => {
        render(
            <I18nProvider>
                <div />
            </I18nProvider>,
        );

        await waitFor(() => expect(loadAntdLocale).toHaveBeenCalledWith('de'));
    });
});
