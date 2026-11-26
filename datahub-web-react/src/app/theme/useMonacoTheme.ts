import { useMonaco } from '@monaco-editor/react';
import type { BeforeMount, Monaco } from '@monaco-editor/react';
import { useCallback, useEffect } from 'react';
import { useTheme } from 'styled-components';

import '@conf/monaco';
import ColorTheme from '@conf/theme/colorThemes/types';

const MONACO_THEME_NAME = 'datahub';
const DARK_THEME_ID = 'themeV2Dark';

/**
 * Maps semantic tokens onto the Monaco workbench color keys. Only the chrome is
 * mapped — syntax token colors come from Monaco's own `vs` / `vs-dark` base via
 * `inherit`, which already reads correctly against these backgrounds.
 *
 * @param colors - Semantic color tokens for the active theme
 * @returns Monaco color overrides keyed by workbench color id
 */
function buildMonacoColors(colors: ColorTheme): Record<string, string> {
    return {
        'editor.background': colors.bg,
        'editor.foreground': colors.text,
        'editor.lineHighlightBackground': colors.bgHover,
        'editor.selectionBackground': colors.bgSurfaceBrand,
        'editorCursor.foreground': colors.textBrand,
        'editorLineNumber.foreground': colors.textTertiary,
        'editorLineNumber.activeForeground': colors.text,
        'editorWidget.background': colors.bgSurface,
        'editorWidget.border': colors.border,
    };
}

export interface MonacoThemeProps {
    theme: string;
    beforeMount: BeforeMount;
}

/**
 * Builds a Monaco theme from the app's semantic tokens.
 *
 * Monaco ships its own light and dark palettes and silently falls back to the
 * light one when no theme is set, which is why every editor renders white
 * regardless of the app theme. Registering in `beforeMount` rather than an
 * effect matters: child effects run before parent ones, so an effect-only
 * version would let the editor paint light for a frame before being corrected.
 *
 * @returns Props to spread onto `<Editor />`
 */
export function useMonacoTheme(): MonacoThemeProps {
    const theme = useTheme();
    const monaco = useMonaco();

    const defineMonacoTheme = useCallback(
        (instance: Monaco) => {
            instance.editor.defineTheme(MONACO_THEME_NAME, {
                base: theme.id === DARK_THEME_ID ? 'vs-dark' : 'vs',
                inherit: true,
                rules: [],
                colors: buildMonacoColors(theme.colors),
            });
        },
        [theme],
    );

    // Re-activate the redefined theme so every live editor follows a dark-mode toggle.
    useEffect(() => {
        if (monaco) {
            defineMonacoTheme(monaco);
            monaco.editor.setTheme(MONACO_THEME_NAME);
        }
    }, [monaco, defineMonacoTheme]);

    return { theme: MONACO_THEME_NAME, beforeMount: defineMonacoTheme };
}
