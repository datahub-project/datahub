import React from 'react';

import { TabButtonItem } from '@components/components/ButtonTabs/types';
import { CODE_BLOCK_EDITOR_DEFAULT_MAX_HEIGHT } from '@components/components/CodeBlock/defaults';
import { CodeBlockLanguageOption, CodeBlockProps } from '@components/components/CodeBlock/types';
import { composeValidateCodeMessage } from '@components/components/CodeBlock/validateCode';

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

/**
 * Shows the writable editor when `onChange` is provided, unless `isReadOnly`.
 * Disabled fields still use the editor chrome with the textarea locked.
 *
 * @param isReadOnly - Explicit read-only override
 * @param onChange - Edit callback; its presence opts into the editor
 * @returns Whether the code body should render as an editor
 */
export function isCodeBlockEditable({
    isReadOnly,
    onChange,
}: {
    isReadOnly?: boolean;
    onChange?: (code: string) => void;
}): boolean {
    return !!onChange && isReadOnly !== true;
}

/**
 * Resolves the code body max height. Writable editors default to a cap so long
 * pastes scroll inside the block; pass `"none"` to grow with content.
 *
 * @param isEditable - Writable editor is active
 * @param maxHeight - Explicit override from props
 * @returns CSS max-height value, or undefined when unbounded
 */
export function resolveCodeBlockBodyMaxHeight({
    isEditable,
    maxHeight,
}: {
    isEditable: boolean;
    maxHeight?: number | string;
}): number | string | undefined {
    if (maxHeight !== undefined) {
        return maxHeight;
    }
    return isEditable ? CODE_BLOCK_EDITOR_DEFAULT_MAX_HEIGHT : undefined;
}

/**
 * Whether the code body should scroll inside a fixed cap.
 *
 * @param maxHeight - Resolved max height from {@link resolveCodeBlockBodyMaxHeight}
 * @returns False when unset or `"none"`
 */
export function isCodeBlockHeightCapped(maxHeight?: number | string): boolean {
    return maxHeight !== undefined && maxHeight !== 'none';
}

/**
 * Copies textarea scroll offsets onto the Prism overlay so they stay aligned.
 *
 * @param textarea - Editor textarea (the scroll source)
 * @param highlight - Overlay layer that mirrors scroll
 */
export function syncCodeBlockHighlightScroll(
    textarea: HTMLTextAreaElement | null,
    highlight: HTMLElement | null,
): void {
    if (!textarea || !highlight) {
        return;
    }
    const overlay = highlight;
    overlay.scrollTop = textarea.scrollTop;
    overlay.scrollLeft = textarea.scrollLeft;
}

/**
 * Whether the header Format action should render.
 *
 * @param showFormat - Explicit override; defaults to true while editable
 * @param isEditable - Writable editor is active
 * @returns True when the Format button should show
 */
export function shouldShowFormatButton({
    showFormat,
    isEditable,
}: {
    showFormat?: boolean;
    isEditable: boolean;
}): boolean {
    return isEditable && showFormat !== false;
}

/**
 * Whether the header Validate action should render.
 *
 * @param validateSyntax - Opt-in flag from props
 * @param isEditable - Writable editor is active
 * @returns True when the Validate button should show
 */
export function shouldShowValidateButton({
    validateSyntax,
    isEditable,
}: {
    validateSyntax?: boolean;
    isEditable: boolean;
}): boolean {
    return isEditable && !!validateSyntax;
}

/**
 * Shift+Alt+F (VS Code format document) — ignored when meta/ctrl is held.
 *
 * @param event - Keyboard event fields
 * @returns True when the shortcut should format
 */
export function isCodeFormatShortcut(event: {
    key: string;
    shiftKey: boolean;
    altKey: boolean;
    metaKey: boolean;
    ctrlKey: boolean;
}): boolean {
    return event.shiftKey && event.altKey && !event.metaKey && !event.ctrlKey && event.key.toLowerCase() === 'f';
}

/**
 * Inserts text at the current textarea selection (used for Tab indent).
 *
 * @param value - Current editor value
 * @param selectionStart - Selection start index
 * @param selectionEnd - Selection end index
 * @param insert - Text to insert
 * @returns Next value and caret position after the insert
 */
export function insertTextAtSelection(
    value: string,
    selectionStart: number,
    selectionEnd: number,
    insert: string,
): { value: string; caret: number } {
    return {
        value: `${value.slice(0, selectionStart)}${insert}${value.slice(selectionEnd)}`,
        caret: selectionStart + insert.length,
    };
}

export function shouldShowCodeBlockHeader({
    showHeader,
    showCopy,
    showFormat,
    showValidate,
    languageOptions,
    headerLeft,
    headerRight,
    languageLabel,
}: {
    showHeader: boolean;
    showCopy: boolean;
    showFormat?: boolean;
    showValidate?: boolean;
    languageOptions?: CodeBlockLanguageOption[];
    headerLeft?: React.ReactNode;
    headerRight?: React.ReactNode;
    languageLabel?: string | false;
}): boolean {
    return (
        showHeader ||
        showCopy ||
        !!showFormat ||
        !!showValidate ||
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

export type CodeBlockStatusDisplay = {
    displayError: string;
    displayWarning: string;
    /** Extra bullet lines under the primary status message (Validate findings). */
    statusDetails: string[];
    isInvalid: boolean;
    hasWarning: boolean;
};

/**
 * Merges parent error/warning props with optional built-in syntax findings.
 * Parent `error` always wins; SQL syntax findings are soft warnings.
 *
 * @param error - Parent hard error message
 * @param warning - Parent soft warning message
 * @param isInvalid - Parent invalid flag
 * @param syntaxSeverity - Built-in syntax finding, if any
 * @param syntaxDetails - Parser / heuristic hints from Validate
 * @param syntaxErrorMessage - Copy for hard syntax failures
 * @param syntaxWarningMessage - Copy for soft SQL warnings
 * @returns Resolved chrome + message state
 */
export function resolveCodeBlockStatusDisplay({
    error,
    warning,
    isInvalid = false,
    syntaxSeverity,
    syntaxDetails = [],
    syntaxErrorMessage,
    syntaxWarningMessage,
}: {
    error?: string;
    warning?: string;
    isInvalid?: boolean;
    syntaxSeverity?: 'error' | 'warning' | null;
    syntaxDetails?: string[];
    syntaxErrorMessage: string;
    syntaxWarningMessage: string;
}): CodeBlockStatusDisplay {
    const parentError = error?.trim() ?? '';
    const parentWarning = warning?.trim() ?? '';
    const trimmedDetails = syntaxDetails.map((d) => d.trim()).filter(Boolean);

    let displayError = parentError;
    let displayWarning = '';
    let statusDetails: string[] = [];

    if (!displayError && syntaxSeverity === 'error') {
        if (trimmedDetails.length <= 1) {
            displayError = composeValidateCodeMessage(syntaxErrorMessage, trimmedDetails[0]);
        } else {
            displayError = syntaxErrorMessage;
            statusDetails = trimmedDetails;
        }
    }

    if (!displayError) {
        if (parentWarning) {
            displayWarning = parentWarning;
        } else if (syntaxSeverity === 'warning') {
            if (trimmedDetails.length <= 1) {
                displayWarning = composeValidateCodeMessage(syntaxWarningMessage, trimmedDetails[0]);
            } else {
                displayWarning = syntaxWarningMessage;
                statusDetails = trimmedDetails;
            }
        }
    }

    const resolvedInvalid = isInvalid || !!displayError;

    return {
        displayError,
        displayWarning,
        statusDetails,
        isInvalid: resolvedInvalid,
        hasWarning: !resolvedInvalid && !!displayWarning,
    };
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
