import { useMonaco } from '@monaco-editor/react';
import type { Monaco } from '@monaco-editor/react';
import { renderHook } from '@testing-library/react-hooks';
import { useTheme } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useMonacoTheme } from '@app/theme/useMonacoTheme';
import themes from '@conf/theme/themes';

vi.mock('@monaco-editor/react', () => ({
    useMonaco: vi.fn(),
    // `@conf/monaco` runs `loader.config(...)` as an import side effect.
    loader: { config: vi.fn() },
}));

vi.mock('styled-components', async () => {
    const actual = await vi.importActual<typeof import('styled-components')>('styled-components');
    return { ...actual, useTheme: vi.fn() };
});

// Helpers

const MONACO_THEME_NAME = 'datahub';

function createMonaco() {
    return { editor: { defineTheme: vi.fn(), setTheme: vi.fn() } };
}

type MonacoStub = ReturnType<typeof createMonaco>;

function asMonaco(stub: MonacoStub): Monaco {
    return stub as unknown as Monaco;
}

function themeDefinition(stub: MonacoStub, call = 0) {
    return stub.editor.defineTheme.mock.calls[call][1];
}

// Setup

beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useTheme).mockReturnValue(themes.themeV2);
    vi.mocked(useMonaco).mockReturnValue(null);
});

describe('useMonacoTheme', () => {
    it('returns the name of the theme it registers', () => {
        const { result } = renderHook(() => useMonacoTheme());
        expect(result.current.theme).toBe(MONACO_THEME_NAME);
    });
});

// Theme registration via beforeMount

describe('useMonacoTheme – beforeMount', () => {
    it('registers against the light Monaco base for the light app theme', () => {
        const monaco = createMonaco();
        const { result } = renderHook(() => useMonacoTheme());

        result.current.beforeMount(asMonaco(monaco));

        expect(themeDefinition(monaco).base).toBe('vs');
    });

    it('registers against the dark Monaco base for the dark app theme', () => {
        vi.mocked(useTheme).mockReturnValue(themes.themeV2Dark);
        const monaco = createMonaco();
        const { result } = renderHook(() => useMonacoTheme());

        result.current.beforeMount(asMonaco(monaco));

        expect(themeDefinition(monaco).base).toBe('vs-dark');
    });

    it('inherits from the base so syntax token colors keep coming from Monaco', () => {
        const monaco = createMonaco();
        const { result } = renderHook(() => useMonacoTheme());

        result.current.beforeMount(asMonaco(monaco));

        expect(themeDefinition(monaco)).toMatchObject({ inherit: true, rules: [] });
    });

    it('maps editor chrome onto the light theme tokens', () => {
        const monaco = createMonaco();
        const { result } = renderHook(() => useMonacoTheme());

        result.current.beforeMount(asMonaco(monaco));

        const { colors } = themes.themeV2;
        expect(themeDefinition(monaco).colors).toEqual({
            'editor.background': colors.bg,
            'editor.foreground': colors.text,
            'editor.lineHighlightBackground': colors.bgHover,
            'editor.selectionBackground': colors.bgSurfaceBrand,
            'editorCursor.foreground': colors.textBrand,
            'editorLineNumber.foreground': colors.textTertiary,
            'editorLineNumber.activeForeground': colors.text,
            'editorWidget.background': colors.bgSurface,
            'editorWidget.border': colors.border,
        });
    });

    it('maps editor chrome onto the dark theme tokens', () => {
        vi.mocked(useTheme).mockReturnValue(themes.themeV2Dark);
        const monaco = createMonaco();
        const { result } = renderHook(() => useMonacoTheme());

        result.current.beforeMount(asMonaco(monaco));

        const definedColors = themeDefinition(monaco).colors;
        expect(definedColors['editor.background']).toBe(themes.themeV2Dark.colors.bg);
        expect(definedColors['editor.background']).not.toBe(themes.themeV2.colors.bg);
    });
});

// Keeping already-mounted editors in sync

describe('useMonacoTheme – live editors', () => {
    it('defines and activates the theme once Monaco has loaded', () => {
        const monaco = createMonaco();
        vi.mocked(useMonaco).mockReturnValue(asMonaco(monaco));

        renderHook(() => useMonacoTheme());

        expect(monaco.editor.defineTheme).toHaveBeenCalledOnce();
        expect(monaco.editor.setTheme).toHaveBeenCalledWith(MONACO_THEME_NAME);
    });

    it('waits for Monaco rather than throwing when it has not loaded yet', () => {
        const { result } = renderHook(() => useMonacoTheme());
        expect(result.error).toBeUndefined();
    });

    it('re-registers and re-activates the theme when the app theme changes', () => {
        const monaco = createMonaco();
        vi.mocked(useMonaco).mockReturnValue(asMonaco(monaco));

        const { rerender } = renderHook(() => useMonacoTheme());
        expect(themeDefinition(monaco).base).toBe('vs');

        vi.mocked(useTheme).mockReturnValue(themes.themeV2Dark);
        rerender();

        expect(themeDefinition(monaco, 1).base).toBe('vs-dark');
        expect(monaco.editor.setTheme).toHaveBeenCalledTimes(2);
    });

    it('does not re-activate the theme on a render that leaves the theme unchanged', () => {
        const monaco = createMonaco();
        vi.mocked(useMonaco).mockReturnValue(asMonaco(monaco));

        const { rerender } = renderHook(() => useMonacoTheme());
        rerender();

        expect(monaco.editor.setTheme).toHaveBeenCalledOnce();
    });
});
