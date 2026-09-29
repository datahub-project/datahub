import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import I18nProvider from '@app/i18n/context/I18nProvider';
import { useLanguageSync } from '@app/i18n/hooks/useLanguageSync';
import { useLocaleConfig } from '@app/i18n/hooks/useLocaleConfig';

vi.mock('@app/i18n/hooks/useLanguageSync');
vi.mock('@app/i18n/hooks/useLocaleConfig');

const mockAntdLocale = { locale: 'en' } as any;

describe('I18nProvider', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(useLanguageSync).mockReturnValue(undefined);
        vi.mocked(useLocaleConfig).mockReturnValue({ lang: 'en', label: 'English', antd: mockAntdLocale, dayjs: 'en' });
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

    it('uses locale from useLocaleConfig for ConfigProvider', () => {
        render(
            <I18nProvider>
                <div />
            </I18nProvider>,
        );

        expect(useLocaleConfig).toHaveBeenCalled();
    });
});
