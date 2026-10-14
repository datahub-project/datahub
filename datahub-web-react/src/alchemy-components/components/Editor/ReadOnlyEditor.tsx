import React, { useMemo } from 'react';
import styled from 'styled-components';

import {
    EDITOR_CONTENT_CLASS,
    editorContentCss,
    readOnlyMarkdownCss,
} from '@components/components/Editor/editorContentStyles';
import { readOnlyHtmlToReact } from '@components/components/Editor/readOnlyContent';
import { toReadOnlyHtml } from '@components/components/Editor/readOnlyHtml';
import { EditorProps } from '@components/components/Editor/types';

const ReadOnlyRoot = styled.div<{
    $hideBorder?: boolean;
    $compact?: boolean;
    $readOnly?: boolean;
}>`
    font-weight: 400;
    display: flex;
    flex: 1 1 auto;
    border: ${(props) => (props.$readOnly || props.$hideBorder ? 'none' : `1px solid ${props.theme.colors.border}`)};
    border-radius: 12px;

    .remirror-theme,
    .remirror-editor-wrapper {
        flex: 1 1 100%;
        display: flex;
        flex-direction: column;
        max-width: 100%;
    }

    /* :where() keeps these defaults below caller overrides that target the same
       content class (summary padding, compact paragraph margin and line limit). */
    :where(.${EDITOR_CONTENT_CLASS}) {
        ${editorContentCss}
        ${readOnlyMarkdownCss}
    }
`;

const Placeholder = styled.div`
    color: ${(props) => props.theme.colors.textDisabled};
`;

export function ReadOnlyEditor(props: EditorProps) {
    const { content, className, placeholder, dataTestId, onKeyDown, onPaste, hideBorder, compact } = props;
    const html = useMemo(() => toReadOnlyHtml(content ?? ''), [content]);
    const contentNodes = useMemo(() => readOnlyHtmlToReact(html), [html]);
    const isEmpty = !content?.trim();
    const contentClassName = `${EDITOR_CONTENT_CLASS} remirror-editor ant-typography`;

    return (
        <ReadOnlyRoot
            className={className}
            data-testid={dataTestId}
            $readOnly
            $hideBorder={hideBorder}
            $compact={compact}
            onKeyDownCapture={onKeyDown}
            onPasteCapture={onPaste}
        >
            {/* Class names match the editable shell so existing viewer CSS (line limits,
                announcement padding) still applies. This node is not ProseMirror. */}
            <div className="remirror-theme">
                <div className="remirror-editor-wrapper">
                    {isEmpty ? (
                        <Placeholder className={contentClassName}>{placeholder}</Placeholder>
                    ) : (
                        <div className={contentClassName}>{contentNodes}</div>
                    )}
                </div>
            </div>
        </ReadOnlyRoot>
    );
}
