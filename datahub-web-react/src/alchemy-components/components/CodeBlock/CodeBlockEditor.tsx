import React, { useCallback, useLayoutEffect, useRef } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';

import {
    CodeBlockEditorFrame,
    CodeBlockHighlightLayer,
    CodeBlockPlaceholder,
    CodeBlockTextarea,
} from '@components/components/CodeBlock/components';
import { CODE_BLOCK_TAB_INDENT } from '@components/components/CodeBlock/defaults';
import { PrismStyle } from '@components/components/CodeBlock/prismTheme';
import {
    insertTextAtSelection,
    isCodeFormatShortcut,
    syncCodeBlockHighlightScroll,
} from '@components/components/CodeBlock/utils';

type Props = {
    code: string;
    language: string;
    wrap: boolean;
    isDisabled?: boolean;
    placeholder?: string;
    customStyle?: React.CSSProperties;
    prismStyle: PrismStyle;
    maxHeight?: number | string;
    onChange: (code: string) => void;
    onRequestFormat?: () => void;
    dataTestId?: string;
    ariaLabel?: string;
};

/**
 * Highlighted textarea that stays visually aligned with the read-only Prism body.
 *
 * @param code - Controlled editor value
 * @param language - Prism language id
 * @param wrap - Soft-wrap long lines
 * @param isDisabled - Locks the textarea
 * @param placeholder - Shown when `code` is empty
 * @param customStyle - Forwarded to Prism
 * @param prismStyle - Theme-aware Prism token colors
 * @param maxHeight - Caps the editor and scrolls overlay + textarea together
 * @param onChange - Emits the next source value
 * @param onRequestFormat - Shift+Alt+F
 * @param dataTestId - Test id for the textarea
 * @param ariaLabel - Accessible name for the editor
 */
export function CodeBlockEditor({
    code,
    language,
    wrap,
    isDisabled,
    placeholder,
    customStyle,
    prismStyle,
    maxHeight,
    onChange,
    onRequestFormat,
    dataTestId,
    ariaLabel,
}: Props): React.ReactElement {
    const textareaRef = useRef<HTMLTextAreaElement>(null);
    const highlightRef = useRef<HTMLDivElement>(null);
    const pendingCaretRef = useRef<number | null>(null);
    const isCapped = maxHeight !== undefined;

    const resizeTextarea = useCallback(() => {
        const el = textareaRef.current;
        if (!el) {
            return;
        }
        const { scrollTop, scrollLeft } = el;
        el.style.height = 'auto';
        el.style.height = `${el.scrollHeight}px`;
        el.scrollTop = scrollTop;
        el.scrollLeft = scrollLeft;
        syncCodeBlockHighlightScroll(el, highlightRef.current);
    }, []);

    useLayoutEffect(() => {
        resizeTextarea();
        const caret = pendingCaretRef.current;
        const el = textareaRef.current;
        if (caret !== null && el) {
            el.setSelectionRange(caret, caret);
            pendingCaretRef.current = null;
        }
        syncCodeBlockHighlightScroll(el, highlightRef.current);
    }, [code, wrap, maxHeight, resizeTextarea]);

    const handleChange = useCallback(
        (event: React.ChangeEvent<HTMLTextAreaElement>) => {
            onChange(event.target.value);
        },
        [onChange],
    );

    const handleScroll = useCallback((event: React.UIEvent<HTMLTextAreaElement>) => {
        syncCodeBlockHighlightScroll(event.currentTarget, highlightRef.current);
    }, []);

    const handleKeyDown = useCallback(
        (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
            if (isDisabled) {
                return;
            }
            if (isCodeFormatShortcut(event) && onRequestFormat) {
                event.preventDefault();
                onRequestFormat();
                return;
            }
            if (event.key !== 'Tab' || event.shiftKey) {
                return;
            }
            event.preventDefault();
            const { selectionStart, selectionEnd, value } = event.currentTarget;
            const next = insertTextAtSelection(value, selectionStart, selectionEnd, CODE_BLOCK_TAB_INDENT);
            pendingCaretRef.current = next.caret;
            onChange(next.value);
        },
        [isDisabled, onChange, onRequestFormat],
    );

    return (
        <CodeBlockEditorFrame>
            <CodeBlockHighlightLayer ref={highlightRef} $scroll={isCapped} aria-hidden="true">
                <SyntaxHighlighter
                    language={language}
                    style={prismStyle}
                    showLineNumbers={false}
                    wrapLongLines={wrap}
                    customStyle={customStyle}
                >
                    {code.endsWith('\n') ? code : `${code}\n`}
                </SyntaxHighlighter>
            </CodeBlockHighlightLayer>
            {!code && placeholder ? <CodeBlockPlaceholder>{placeholder}</CodeBlockPlaceholder> : null}
            <CodeBlockTextarea
                ref={textareaRef}
                value={code}
                onChange={handleChange}
                onScroll={handleScroll}
                onKeyDown={handleKeyDown}
                $wrap={wrap}
                $disabled={isDisabled}
                $maxHeight={maxHeight}
                $scroll={isCapped}
                disabled={isDisabled}
                spellCheck={false}
                autoComplete="off"
                aria-label={ariaLabel}
                data-testid={dataTestId}
            />
        </CodeBlockEditorFrame>
    );
}
