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
    showFormat: boolean;
}

export interface CodeBlockProps
    extends Partial<CodeBlockPropsDefaults>,
        Omit<HTMLAttributes<HTMLDivElement>, 'onCopy' | 'onChange'> {
    /** Source code to display, or the controlled value when writable */
    code: string;

    /**
     * Called as the user edits. Providing this enables the writable editor
     * unless `isReadOnly` is set.
     */
    onChange?: (code: string) => void;

    /**
     * Force a read-only highlighter even when `onChange` is provided.
     * Defaults to read-only when `onChange` is omitted.
     */
    isReadOnly?: boolean;

    /** Disable editing without switching back to the read-only highlighter */
    isDisabled?: boolean;

    /** Placeholder shown when the writable editor is empty */
    placeholder?: string;

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

    /**
     * Max height of the code body. Read-only uses this on the content pane.
     * Writable editors default to 400px and scroll inside the overlay; pass `"none"` to grow with content.
     */
    maxHeight?: number | string;

    overflow?: 'auto' | 'hidden';

    /**
     * `card` — bordered panel with shadow (chat / metric SQL look).
     * `embedded` — transparent, no outer chrome (nest inside existing surfaces).
     */
    variant?: CodeBlockVariant;

    /** Additional styles forwarded to Prism's `customStyle` */
    customStyle?: CSSProperties;

    /**
     * Show a Format action in the header when the editor is writable.
     * SQL (including dialects), JSON, YAML, and GraphQL are pretty-printed;
     * other languages get whitespace tidying.
     */
    showFormat?: boolean;

    /**
     * When true on a writable editor, shows a Validate action in the header.
     * Clicking it runs a syntax check and lists any problems found.
     * JSON / YAML / GraphQL surface as errors; SQL surfaces as a soft warning.
     * Does not block editing. Off by default so chat / read surfaces stay light.
     */
    validateSyntax?: boolean;

    /** Hard validation message from the parent (implies invalid border). */
    error?: string;

    /** Soft validation message from the parent (warning border when not invalid). */
    warning?: string;

    /** Force invalid border without a message; also true when `error` is set. */
    isInvalid?: boolean;

    /** Click handler on the code body (e.g. expand query card). Ignored while writable. */
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
    $editable?: boolean;
};
