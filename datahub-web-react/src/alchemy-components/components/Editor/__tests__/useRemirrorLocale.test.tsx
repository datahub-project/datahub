import { MockedProvider } from '@apollo/client/testing';
import { i18n as remirrorI18n } from '@remirror/i18n';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import i18next from 'i18next';
import React from 'react';
import { I18nextProvider, initReactI18next } from 'react-i18next';
import { ThemeProvider } from 'styled-components';
import { afterEach, describe, expect, it } from 'vitest';

import { Editor } from '@components/components/Editor/EditorImpl';

import themeV2 from '@conf/theme/themeV2';

async function renderEditor(language: string): Promise<void> {
    const appI18n = i18next.createInstance();
    await appI18n.use(initReactI18next).init({
        lng: language,
        fallbackLng: 'en',
        resources: {},
        react: { useSuspense: false },
    });

    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <I18nextProvider i18n={appI18n}>
                <ThemeProvider theme={themeV2}>
                    <Editor content="" onChange={() => {}} />
                </ThemeProvider>
            </I18nextProvider>
        </MockedProvider>,
    );

    await screen.findByTestId('command-toggleBold-btn');
}

async function tooltipFor(testId: string): Promise<string> {
    await userEvent.hover(screen.getByTestId(testId));

    let text = '';
    await waitFor(() => {
        const tooltip = document.querySelector('.ant-tooltip:not(.ant-tooltip-hidden) .ant-tooltip-inner');
        expect(tooltip?.textContent?.trim()).toBeTruthy();
        text = tooltip?.textContent?.trim() ?? '';
    });
    return text;
}

describe('Remirror editor localization', () => {
    // Antd renders tooltips into body portals that survive React Testing Library's cleanup, so a
    // stale tooltip from the previous case would otherwise be the one we read back.
    afterEach(() => {
        cleanup();
        document.querySelectorAll('.ant-tooltip').forEach((node) => node.remove());
    });

    // `<Remirror>` renders its own `I18nProvider` from its `i18n`/`locale` props, so wrapping the
    // tree in an outer provider is silently shadowed and every built-in label stays English.
    it('leaves labels in English for a language we ship no bundle for', async () => {
        await renderEditor('ko');

        expect(await tooltipFor('command-toggleBold-btn')).toBe('Bold');
    });

    it('localizes built-in toolbar labels for the active app language', async () => {
        await renderEditor('zh-TW');

        expect(await tooltipFor('command-toggleBold-btn')).toBe('粗體');
    });

    it('uses the region-specific bundle rather than a shared zh one', async () => {
        await renderEditor('zh-CN');

        expect(await tooltipFor('command-toggleBold-btn')).toBe('加粗');
    });

    // Lingui keys plural rules by language only (`pt`, `zh`), so a region-tagged locale has to
    // fall back to its primary subtag. Without that, every plural resolves to the `other` branch
    // and the table row counter reads "1 linhas".
    it('applies primary-subtag plural rules to a region-tagged locale', async () => {
        await renderEditor('pt-BR');

        await waitFor(() => expect(remirrorI18n.locale).toBe('pt-BR'));
        expect(remirrorI18n._({ id: 'extension.table.row_count', values: { count: 1 } })).toBe('1 linha');
        expect(remirrorI18n._({ id: 'extension.table.row_count', values: { count: 3 } })).toBe('3 linhas');
    });
});
