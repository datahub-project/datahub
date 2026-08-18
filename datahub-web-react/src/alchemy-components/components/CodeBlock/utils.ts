import React from 'react';

import { TabButtonItem } from '@components/components/ButtonTabs/types';
import { CodeBlockLanguageOption, CodeBlockProps } from '@components/components/CodeBlock/types';

export type LanguageControlMode = 'static' | 'tabs' | 'select';

export function isLineHighlighted(highlightedLines: CodeBlockProps['highlightedLines'], lineNumber: number): boolean {
    if (!highlightedLines) {
        return false;
    }
    if (Array.isArray(highlightedLines)) {
        return highlightedLines.includes(lineNumber);
    }
    return highlightedLines.has(lineNumber);
}

export function shouldShowCodeBlockHeader({
    showHeader,
    showCopy,
    languageOptions,
    headerLeft,
    headerRight,
    languageLabel,
}: {
    showHeader: boolean;
    showCopy: boolean;
    languageOptions?: CodeBlockLanguageOption[];
    headerLeft?: React.ReactNode;
    headerRight?: React.ReactNode;
    languageLabel?: string | false;
}): boolean {
    return (
        showHeader ||
        showCopy ||
        (languageOptions?.length ?? 0) > 0 ||
        !!headerLeft ||
        !!headerRight ||
        typeof languageLabel === 'string'
    );
}

export function resolveLanguageLabel({
    language,
    languageLabel,
    languageOptions,
    selectedLanguage,
    hasHeaderLeft,
}: {
    language: string;
    languageLabel?: string | false;
    languageOptions?: CodeBlockLanguageOption[];
    selectedLanguage?: string;
    hasHeaderLeft: boolean;
}): string | null {
    if (languageLabel === false) {
        return null;
    }
    if (typeof languageLabel === 'string') {
        return languageLabel;
    }
    // Parents that pass headerLeft (pills/badges) own that slot.
    if (hasHeaderLeft) {
        return null;
    }
    if (languageOptions?.length) {
        return (
            languageOptions.find((option) => option.value === selectedLanguage)?.label ??
            languageOptions[0]?.label ??
            null
        );
    }
    return language.toUpperCase();
}

export function shouldShowLineNumbers({
    showLineNumbers,
    hideLineNumbers,
    highlightedLines,
}: {
    showLineNumbers: boolean;
    hideLineNumbers: boolean;
    highlightedLines?: CodeBlockProps['highlightedLines'];
}): boolean {
    return showLineNumbers || hideLineNumbers || !!highlightedLines;
}

export function getCodeBlockTestId(dataTestId: string | undefined, suffix: string, fallback: string): string {
    return dataTestId ? `${dataTestId}-${suffix}` : fallback;
}

export function getLanguageControlMode(
    options: CodeBlockLanguageOption[],
    onLanguageChange?: (value: string) => void,
): LanguageControlMode {
    if (!onLanguageChange || options.length <= 1) {
        return 'static';
    }
    if (options.length === 2) {
        return 'tabs';
    }
    return 'select';
}

export function resolveStaticLanguageLabel(
    options: CodeBlockLanguageOption[],
    selectedLanguage: string | undefined,
    staticLabel: string | null,
): string | null {
    return (
        staticLabel ?? options.find((option) => option.value === selectedLanguage)?.label ?? options[0]?.label ?? null
    );
}

export function mapLanguageOptionsToTabItems(options: CodeBlockLanguageOption[], dataTestId?: string): TabButtonItem[] {
    return options.map((option) => ({
        key: option.value,
        label: option.label,
        dataTestId: dataTestId ? `${dataTestId}-${option.value}` : undefined,
    }));
}

export function mergeHighlightedLineProps(
    lineNumber: number,
    highlightedLines: CodeBlockProps['highlightedLines'],
    highlightBackgroundColor: string,
    linePropsProp?: (lineNumber: number) => React.HTMLProps<HTMLElement>,
): React.HTMLProps<HTMLElement> {
    const external = linePropsProp?.(lineNumber) ?? {};
    if (!isLineHighlighted(highlightedLines, lineNumber)) {
        return external;
    }
    return {
        ...external,
        style: {
            display: 'block',
            backgroundColor: highlightBackgroundColor,
            ...external.style,
        },
    };
}
