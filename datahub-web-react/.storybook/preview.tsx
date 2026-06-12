import { BADGE, defaultBadgesConfig } from '@geometricpanda/storybook-addon-badges';
// FYI: import of antd styles required to show components based on it correctly
import 'antd/dist/antd.css';
import React, { useEffect } from 'react';
import { I18nextProvider } from 'react-i18next';
import { ThemeProvider } from 'styled-components';

import { LOCALE_MAP } from '../src/app/i18n/constants';
import themes from '../src/conf/theme/themes';
import DocTemplate from './DocTemplate.mdx';
import i18n from './i18n';
import './storybook-theme.css';

// Drives the shared i18next instance from the toolbar's selected locale. Done in an
// effect so we mutate the singleton after render rather than during it.
const LocaleProvider = ({ locale, children }: { locale: string; children: React.ReactNode }) => {
    useEffect(() => {
        if (i18n.language !== locale) {
            i18n.changeLanguage(locale);
        }
    }, [locale]);
    return <>{children}</>;
};

const preview = {
    tags: ['!dev', 'autodocs'],
    // Wrap every story in the providers the components expect at runtime:
    // - ThemeProvider: so styled-components reading `theme.colors.*` (e.g. textTertiary) resolve
    //   (without it `theme` is empty and those reads throw "Cannot read properties of undefined").
    // - I18nextProvider: so `t()` / <Trans> render real strings instead of raw keys.
    decorators: [
        (Story: React.ComponentType, context: { globals: { locale?: string } }) => (
            <I18nextProvider i18n={i18n}>
                <LocaleProvider locale={context.globals.locale || 'en'}>
                    <ThemeProvider theme={themes.themeV2}>
                        <Story />
                    </ThemeProvider>
                </LocaleProvider>
            </I18nextProvider>
        ),
    ],
    // Toolbar dropdown to switch the active language across all stories.
    globalTypes: {
        locale: {
            name: 'Locale',
            description: 'Active language',
            defaultValue: 'en',
            toolbar: {
                icon: 'globe',
                items: Object.values(LOCALE_MAP).map(({ lang, label }) => ({ value: lang, title: label })),
                dynamicTitle: true,
            },
        },
    },
    parameters: {
        previewTabs: {
            'storybook/docs/panel': { index: -1 },
        },
        controls: {
            matchers: {
                color: /(background|color)$/i,
                date: /Date$/i,
            },
        },
        options: {
            storySort: {
                method: 'alphabetical',
                order: [
                    // Order of Docs Pages
                    'Introduction',
                    'Style Guide',
                    'Design Tokens',
                    'Style Utilities',
                    'Icons',

                    // Order of Components
                    'Layout',
                    'Forms',
                    'Data Display',
                    'Feedback',
                    'Typography',
                    'Overlay',
                    'Disclosure',
                    'Navigation',
                    'Media',
                    'Other',
                ],
                locales: '',
            },
        },
        docs: {
            page: DocTemplate,
            toc: {
                disable: false,
            },
            docs: {
                source: {
                    format: true,
                },
            },
        },

        // Reconfig the premade badges with better titles
        badgesConfig: {
            stable: {
                ...defaultBadgesConfig[BADGE.STABLE],
                title: 'Stable',
                tooltip: 'This component is stable but may have frequent changes. Use at own discretion.',
            },
            productionReady: {
                ...defaultBadgesConfig[BADGE.STABLE],
                title: 'Production Ready',
                tooltip: 'This component is production ready and has been tested in a production environment.',
            },
            WIP: {
                ...defaultBadgesConfig[BADGE.BETA],
                title: 'WIP',
                tooltip: 'This component is a work in progress and may not be fully functional or tested.',
            },
            readyForDesignReview: {
                ...defaultBadgesConfig[BADGE.NEEDS_REVISION],
                title: 'Ready for Design Review',
                tooltip: 'This component is ready for design review and feedback.',
            },
        },
    },
};

export default preview;
