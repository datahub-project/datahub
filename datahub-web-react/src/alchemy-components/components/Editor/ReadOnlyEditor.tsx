import React, { forwardRef, useMemo } from 'react';
import styled from 'styled-components';

import {
    EDITOR_CONTENT_CLASS,
    editorContentCss,
    readOnlyMarkdownCss,
} from '@components/components/Editor/editorContentStyles';
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

    .${EDITOR_CONTENT_CLASS} {
        ${editorContentCss}
        ${readOnlyMarkdownCss}
    }
`;

const Placeholder = styled.div`
    color: ${(props) => props.theme.colors.textDisabled};
`;

export const ReadOnlyEditor = forwardRef<HTMLDivElement, EditorProps>((props, ref) => {
    const { content, className, placeholder, dataTestId, onKeyDown, onPaste, hideBorder, compact } = props;
    const html = useMemo(() => toReadOnlyHtml(content ?? ''), [content]);
    const isEmpty = !content?.trim();
    const contentClassName = `${EDITOR_CONTENT_CLASS} remirror-editor ant-typography`;

    return (
        <ReadOnlyRoot
            ref={ref}
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
                        <div className={contentClassName} dangerouslySetInnerHTML={{ __html: html }} />
                    )}
                </div>
            </div>
        </ReadOnlyRoot>
    );
});

ReadOnlyEditor.displayName = 'ReadOnlyEditor';
