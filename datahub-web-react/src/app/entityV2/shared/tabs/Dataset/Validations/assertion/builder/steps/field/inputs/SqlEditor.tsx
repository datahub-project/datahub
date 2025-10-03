import { colors } from '@components';
import Editor from '@monaco-editor/react';
import React from 'react';
import styled from 'styled-components';

import { radius } from '@components/theme';

const SqlEditorContainer = styled.div<{ $disabled?: boolean }>`
    width: 100%;
    min-height: 120px;
    border: 1px solid ${colors.gray[100]};
    border-radius: ${radius.md};
    background: ${(props) => (props.$disabled ? colors.gray[1500] : colors.white)};
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07) !important;
    overflow: hidden;

    &:hover {
        box-shadow: 0px 1px 2px 1px rgba(33, 23, 95, 0.07) !important;
    }

    &:focus-within {
        border-color: ${colors.gray[1800]} !important;
        outline: 1px solid ${colors.violet[200]} !important;
    }

    /* Override Monaco editor background when disabled */
    ${(props) =>
        props.$disabled &&
        `
        pointer-events: none !important;
        cursor: not-allowed !important;
        
        .monaco-editor {
            background-color: ${colors.gray[1500]} !important;
            pointer-events: none !important;
            cursor: not-allowed !important;
        }
        .monaco-editor .view-overlays {
            background-color: ${colors.gray[1500]} !important;
            pointer-events: none !important;
        }
        .monaco-editor .margin {
            background-color: ${colors.gray[1500]} !important;
            pointer-events: none !important;
        }
        .monaco-editor .monaco-editor-background {
            background-color: ${colors.gray[1500]} !important;
            pointer-events: none !important;
        }
        .monaco-editor .view-lines {
            pointer-events: none !important;
        }
        .monaco-editor .cursors-layer {
            pointer-events: none !important;
        }
        .monaco-editor .selected-text {
            pointer-events: none !important;
        }
    `}
`;

const getQueryEditorOptions = (isReadOnly: boolean) => {
    const baseOptions = {
        minimap: { enabled: false },
        scrollbar: {
            vertical: 'hidden' as const,
            horizontal: 'hidden' as const,
        },
        readOnly: isReadOnly,
    };

    if (!isReadOnly) {
        return baseOptions;
    }

    return {
        ...baseOptions,
        theme: 'vs' as const,
        backgroundColor: colors.gray[1500],
        editor: {
            background: colors.gray[1500],
            domReadOnly: true,
            readOnly: true,
            dragAndDrop: false,
            links: false,
            contextmenu: false,
            hover: { enabled: false },
            quickSuggestions: false,
            suggestOnTriggerCharacters: false,
            acceptSuggestionOnEnter: 'off' as const,
            tabCompletion: 'off' as const,
            wordBasedSuggestions: 'off' as const,
            parameterHints: { enabled: false },
            selectionClipboard: false,
            mouseWheelZoom: false,
            bracketPairColorization: { enabled: false },
            guides: { bracketPairs: false },
            inlineSuggest: { enabled: false },
            linkedEditing: false,
            stickyScroll: { enabled: false },
            autoClosingBrackets: 'never' as const,
            autoClosingQuotes: 'never' as const,
            autoSurround: 'never' as const,
            autoIndent: 'none' as const,
            formatOnPaste: false,
            formatOnType: false,
            formatOnSave: false,
            codeLens: false,
            folding: false,
            showFoldingControls: 'never' as const,
            unfoldOnClickAfterEnd: false,
            glyphMargin: false,
            lineNumbers: 'on' as const,
            renderWhitespace: 'none' as const,
            renderControlCharacters: false,
            renderIndentGuides: false,
            highlightActiveIndentGuide: false,
            renderLineHighlight: 'none' as const,
            selectionHighlight: false,
            occurrencesHighlight: false,
            semanticHighlighting: { enabled: false },
            inlayHints: { enabled: false },
        },
        editorGutter: {
            background: colors.gray[1500],
        },
    };
};

type SqlEditorProps = {
    value: string;
    onChange: (value: string) => void;
    disabled?: boolean;
    height?: string;
    className?: string;
};

export const SqlEditor = ({ value, onChange, disabled = false, height = '120px', className }: SqlEditorProps) => {
    return (
        <SqlEditorContainer $disabled={disabled}>
            <Editor
                options={getQueryEditorOptions(disabled)}
                height={height}
                defaultLanguage="sql"
                value={value}
                onChange={(sqlValue) => onChange(sqlValue || '')}
                className={className}
            />
        </SqlEditorContainer>
    );
};
