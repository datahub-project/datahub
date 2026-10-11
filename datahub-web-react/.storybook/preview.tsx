import { BADGE, defaultBadgesConfig } from '@geometricpanda/storybook-addon-badges';
import { DocsContainer } from '@storybook/blocks';
import { ConfigProvider } from 'antd';
// FYI: import of antd styles required to show components based on it correctly
import 'antd/dist/antd.css';
import React, { useEffect } from 'react';
import { I18nextProvider } from 'react-i18next';
import { ThemeProvider, createGlobalStyle } from 'styled-components';

import { LOCALE_MAP } from '../src/app/i18n/constants';
import { isSupportedLanguage } from '../src/app/i18n/utils';
import GlobalThemeStyles from '../src/app/theme/GlobalThemeStyles';
import themes from '../src/conf/theme/themes';
import dayjs from '../src/utils/dayjs';
import DocTemplate from './DocTemplate.mdx';
import { darkDocsTheme, lightDocsTheme } from './docsThemes';
import i18n from './i18n';
import './storybook-theme.css';

// Drives i18next, antd, and dayjs from the toolbar's selected locale — mirroring the app's
// `I18nProvider`/`useLanguageSync` so antd components (e.g. DatePicker calendar labels) and
// dayjs-formatted dates localize too, not just `t()` strings. i18next is mutated in an effect
// so we touch the singleton after render rather than during it.
const LocaleProvider = ({ locale, children }: { locale: string; children: React.ReactNode }) => {
    const localeConfig = isSupportedLanguage(locale) ? LOCALE_MAP[locale] : LOCALE_MAP.en;
    useEffect(() => {
        if (i18n.language !== localeConfig.lang) {
            i18n.changeLanguage(localeConfig.lang);
        }
        dayjs.locale(localeConfig.dayjs);
    }, [localeConfig.lang, localeConfig.dayjs]);
    return <ConfigProvider locale={localeConfig.antd}>{children}</ConfigProvider>;
};

// Storybook's canvas is always white, so a dark story would otherwise render its
// components on a light page. This has to be a global style rather than a wrapper
// element: `layout: 'centered'` stories sit in a flex container that shrinks a
// wrapper to its content, and portalled components (Toast, Modal, Dropdown) escape
// the story root entirely. Docs-page chrome is handled by the docs theme instead.
//
// Mounted after GlobalThemeStyles so the canvas sits on the plain surface color
// rather than the app's nav background.
const ThemedPreviewStyles = createGlobalStyle<{ $bg: string; $text: string }>`
    html,
    body,
    #storybook-root,
    #storybook-docs,
    .docs-story {
        background: ${(props) => props.$bg};
        color: ${(props) => props.$text};
    }
`;

const DARK_THEME_ID = 'themeV2Dark';

type Globals = Record<string, string | undefined>;

type DocsGlobalsContext = {
    store?: {
        userGlobals?: { globals?: Globals };
        globals?: { get?: () => Globals };
    };
    globals?: Globals;
};

type DocsContainerContext = React.ComponentProps<typeof DocsContainer>['context'];

/**
 * Reads the toolbar's theme global from a docs render context.
 *
 * Storybook has moved where globals live on the docs context across minor
 * versions, so each known shape is tried before falling back to light.
 *
 * @param context - The docs render context Storybook passes to the container
 * @returns The active theme id, defaulting to the light theme
 */
function readThemeGlobal(context: DocsGlobalsContext): string {
    return (
        context?.store?.userGlobals?.globals?.theme ??
        context?.store?.globals?.get?.()?.theme ??
        context?.globals?.theme ??
        'themeV2'
    );
}

const THEME_OPTIONS: Record<string, (typeof themes)['themeV2']> = {
    themeV2: themes.themeV2,
    themeV2Dark: themes.themeV2Dark,
};

function resolveTheme(themeId: string | undefined) {
    return THEME_OPTIONS[themeId ?? 'themeV2'] ?? THEME_OPTIONS.themeV2;
}

/**
 * Supplies the active theme to everything rendered inside the preview iframe.
 *
 * `GlobalThemeStyles` is the sheet `CustomThemeProvider` mounts in the app. antd
 * compiles its palette from LESS at build time, so without it every antd-backed
 * component (Modal, Drawer, Select, Table, Pagination, Timeline…) keeps its light
 * colors in dark mode.
 *
 * @param injectGlobalStyles - Mirrors `CustomThemeProvider`'s prop of the same name.
 *   A docs page renders one decorator per story, so the container injects the sheets
 *   once for the page and the decorator skips them.
 */
function PreviewTheme({
    themeId,
    injectGlobalStyles,
    children,
}: {
    themeId: string | undefined;
    injectGlobalStyles: boolean;
    children: React.ReactNode;
}) {
    const activeTheme = resolveTheme(themeId);
    return (
        <ThemeProvider theme={activeTheme}>
            {injectGlobalStyles && (
                <>
                    <GlobalThemeStyles />
                    <ThemedPreviewStyles $bg={activeTheme.colors.bg} $text={activeTheme.colors.text} />
                </>
            )}
            {children}
        </ThemeProvider>
    );
}

/**
 * Wraps docs pages so Storybook recolors its own prose, tables, and table of
 * contents from the active theme rather than staying pinned to its light palette.
 *
 * This also has to carry the theme itself: the `.docs` pages built from plain MDX
 * (Introduction, Style Guide, Design Tokens, Icons, Contributing) declare no stories,
 * so the decorator never runs for them and the page would keep the iframe's white
 * background — visible as a rail beside `.sbdocs-wrapper`, which is capped at 95% width.
 */
function ThemedDocsContainer({ children, context }: { children: React.ReactNode; context: DocsContainerContext }) {
    const themeId = readThemeGlobal(context as DocsGlobalsContext);
    return (
        <DocsContainer context={context} theme={themeId === DARK_THEME_ID ? darkDocsTheme : lightDocsTheme}>
            <PreviewTheme themeId={themeId} injectGlobalStyles>
                {children}
            </PreviewTheme>
        </DocsContainer>
    );
}

const preview = {
    tags: ['!dev', 'autodocs'],
    // Wrap every story in the providers the components expect at runtime:
    // - ThemeProvider: so styled-components reading `theme.colors.*` (e.g. textTertiary) resolve
    //   (without it `theme` is empty and those reads throw "Cannot read properties of undefined").
    // - I18nextProvider: so `t()` / <Trans> render real strings instead of raw keys.
    decorators: [
        (Story: React.ComponentType, context: { globals: { locale?: string; theme?: string }; viewMode?: string }) => {
            return (
                <I18nextProvider i18n={i18n}>
                    <LocaleProvider locale={context.globals.locale || 'en'}>
                        <PreviewTheme themeId={context.globals.theme} injectGlobalStyles={context.viewMode !== 'docs'}>
                            <Story />
                        </PreviewTheme>
                    </LocaleProvider>
                </I18nextProvider>
            );
        },
    ],
    // Toolbar dropdowns to switch the active language and color theme across all stories.
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
        theme: {
            name: 'Theme',
            description: 'Active color theme',
            defaultValue: 'themeV2',
            toolbar: {
                icon: 'paintbrush',
                items: [
                    { value: 'themeV2', title: 'Light' },
                    { value: 'themeV2Dark', title: 'Dark' },
                ],
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
            container: ThemedDocsContainer,
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
