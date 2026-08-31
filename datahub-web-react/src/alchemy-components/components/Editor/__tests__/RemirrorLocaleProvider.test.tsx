import { i18n as remirrorI18n } from '@remirror/i18n';
import { render, waitFor } from '@testing-library/react';
import i18next from 'i18next';
import React from 'react';
import { I18nextProvider, initReactI18next } from 'react-i18next';
import { describe, expect, it } from 'vitest';

import RemirrorLocaleProvider from '@src/alchemy-components/components/Editor/RemirrorLocaleProvider';

async function activateLanguage(language: string): Promise<void> {
    const appI18n = i18next.createInstance();
    await appI18n.use(initReactI18next).init({
        lng: language,
        fallbackLng: 'en',
        resources: {},
        react: { useSuspense: false },
    });

    render(
        <I18nextProvider i18n={appI18n}>
            <RemirrorLocaleProvider>editor</RemirrorLocaleProvider>
        </I18nextProvider>,
    );

    await waitFor(() => expect(remirrorI18n.locale).toBe(language));
}

function rowCount(count: number): string {
    return remirrorI18n._({ id: 'extension.table.row_count', values: { count } });
}

describe('RemirrorLocaleProvider', () => {
    // Lingui keys plural rules by language only (`pt`, `zh`), so a region-tagged locale has to
    // fall back to its primary subtag. Without that, Lingui resolves every plural to the `other`
    // branch and pt-BR renders "1 linhas".
    it('applies primary-subtag plural rules to a region-tagged locale', async () => {
        await activateLanguage('pt-BR');

        expect(rowCount(1)).toBe('1 linha');
        expect(rowCount(3)).toBe('3 linhas');
    });

    it('activates the region-specific bundle for zh-TW and zh-CN rather than a shared zh one', async () => {
        await activateLanguage('zh-TW');
        expect(rowCount(3)).toBe('3 列');

        await activateLanguage('zh-CN');
        expect(rowCount(3)).toBe('3 行');
    });
});
