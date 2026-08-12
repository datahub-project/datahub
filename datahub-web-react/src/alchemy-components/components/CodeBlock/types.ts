import React, { CSSProperties, HTMLAttributes } from 'react';

import { SelectOption } from '@components/components/Select/types';

export type CodeBlockVariant = 'card' | 'embedded';

export type CodeBlockLanguageOption = SelectOption;

export interface CodeBlockPropsDefaults {
    language: string;
    variant: CodeBlockVariant;
    showHeader: boolean;
    showCopy: boolean;
    showLineNumbers: boolean;
    wrap: boolean;
    overflow: 'auto' | 'hidden';
}

export interface CodeBlockProps
    extends Partial<CodeBlockPropsDefaults>,
        Omit<HTMLAttributes<HTMLDivElement>, 'onCopy'> {
    /** Source code to display */
    code: string;

    /**
     * Prism language id used for highlighting (e.g. `sql`, `yaml`, `json`).
     * Independent of the header label / language select value.
     */
    language?: string;

    /**
     * Label shown in the header when language cannot be changed.
     * Defaults to uppercase `language`, or the selected `languageOptions` label.
     * Pass `false` to hide.
     */
    languageLabel?: string | false;

    /**
     * Available languages/dialects for the header control.
     * Rendering:
     * - no `onLanguageChange` or ≤1 option → static text
     * - exactly 2 options + `onLanguageChange` → tab switch (lineage / home module style)
     * - 3+ options + `onLanguageChange` → SimpleSelect dropdown
     * Parent owns selection via `selectedLanguage` / `onLanguageChange`.
     */
    languageOptions?: CodeBlockLanguageOption[];

    /** Currently selected language option value (controlled with `languageOptions`) */
    selectedLanguage?: string;

    /**
     * Enables language switching when `languageOptions` has 2+ entries.
     * Without this callback, options render as static text only.
     */
    onLanguageChange?: (value: string) => void;

    /** Custom content after the language chrome (pills, badges, format toggles) */
    headerLeft?: React.ReactNode;

    /** Extra actions on the right side of the header, before the copy button */
    headerRight?: React.ReactNode;

    /** Optional footer slot below the code body */
    footer?: React.ReactNode;

    /** Show built-in copy control in the header */
    showCopy?: boolean;

    /** Override clipboard payload (defaults to `code`) */
    copyText?: string;

    /** Controlled copied state; when omitted, component manages it internally */
    isCopied?: boolean;

    /** Fires after a successful copy (CodeBlock already shows an alchemy success toast) */
    onCopy?: () => void;

    /** Show truncation warning banner (chat streaming / max-length cases) */
    isTruncated?: boolean;

    /** Override truncation banner message */
    truncatedMessage?: React.ReactNode;

    /** Show Prism line numbers */
    showLineNumbers?: boolean;

    /**
     * Keep line numbers for `lineProps` / highlights but hide the gutter visually
     * (sidebar SQL previews).
     */
    hideLineNumbers?: boolean;

    /** Soft-wrap long lines */
    wrap?: boolean;

    /** 1-based line numbers to highlight */
    highlightedLines?: number[] | ReadonlySet<number>;

    /** Escape hatch for per-line props (merged with highlight styles) */
    lineProps?: (lineNumber: number) => React.HTMLProps<HTMLElement>;

    /** Max height of the code body; enables scrolling when overflow is auto */
    maxHeight?: number | string;

    overflow?: 'auto' | 'hidden';

    /**
     * `card` — bordered panel with shadow (chat / metric SQL look).
     * `embedded` — transparent, no outer chrome (nest inside existing surfaces).
     */
    variant?: CodeBlockVariant;

    /** Additional styles forwarded to Prism's `customStyle` */
    customStyle?: CSSProperties;

    /** Click handler on the code body (e.g. expand query card) */
    onCodeClick?: () => void;

    /** Override data-testid on the copy button */
    copyDataTestId?: string;

    /** Override data-testid on the code content region */
    contentDataTestId?: string;

    className?: string;
    'data-testid'?: string;
}

export type CodeBlockStyleProps = {
    $variant: CodeBlockVariant;
    $overflow: 'auto' | 'hidden';
    $maxHeight?: number | string;
    $clickable?: boolean;
};
