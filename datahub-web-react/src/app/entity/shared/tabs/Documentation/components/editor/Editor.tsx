import { EditorComponent, Remirror, TableComponents, ThemeProvider, useRemirror } from '@remirror/react';
import DOMPurify from 'dompurify';
import React, { forwardRef, useEffect, useImperativeHandle, useState } from 'react';
import { useMount } from 'react-use';
import {
    BlockquoteExtension,
    BoldExtension,
    BulletListExtension,
    CodeBlockExtension,
    CodeExtension,
    DropCursorExtension,
    HardBreakExtension,
    HeadingExtension,
    HistoryExtension,
    HorizontalRuleExtension,
    ImageExtension,
    ItalicExtension,
    LinkExtension,
    ListItemExtension,
    MarkdownExtension,
    OrderedListExtension,
    StrikeExtension,
    TableExtension,
    UnderlineExtension,
} from 'remirror/extensions';

import { EditorContainer, EditorTheme } from '@app/entity/shared/tabs/Documentation/components/editor/EditorTheme';
import { OnChangeMarkdown } from '@app/entity/shared/tabs/Documentation/components/editor/OnChangeMarkdown';
import { htmlToMarkdown } from '@app/entity/shared/tabs/Documentation/components/editor/extensions/htmlToMarkdown';
import { markdownToHtml } from '@app/entity/shared/tabs/Documentation/components/editor/extensions/markdownToHtml';
import { DataHubMentionsExtension } from '@app/entity/shared/tabs/Documentation/components/editor/extensions/mentions/DataHubMentionsExtension';
import { MentionsComponent } from '@app/entity/shared/tabs/Documentation/components/editor/extensions/mentions/MentionsComponent';
import { CodeBlockToolbar } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/CodeBlockToolbar';
import { FloatingToolbar } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/FloatingToolbar';
import { TableCellMenu } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/TableCellMenu';
import { Toolbar } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/Toolbar';

type EditorProps = {
    readOnly?: boolean;
    content?: string;
    onChange?: (md: string) => void;
    className?: string;
    doNotFocus?: boolean;
    dataTestId?: string;
    onKeyDown?: (event: React.KeyboardEvent<HTMLDivElement>) => void;
    editorStyle?: string;
};

export const Editor = forwardRef((props: EditorProps, ref) => {
    const { content, readOnly, onChange, className, dataTestId, onKeyDown, editorStyle } = props;
    const { manager, state, getContext } = useRemirror({
        extensions: () => [
            new BlockquoteExtension(),
            new BoldExtension({}),
            new BulletListExtension({}),
            new CodeBlockExtension({ syntaxTheme: 'base16_ateliersulphurpool_light' }),
            new CodeExtension(),
            new DataHubMentionsExtension({}),
            new DropCursorExtension({}),
            new HardBreakExtension(),
            new HeadingExtension({}),
            new HistoryExtension({}),
            new HorizontalRuleExtension({}),
            new ImageExtension({ enableResizing: !readOnly }),
            new ItalicExtension(),
            new LinkExtension({ autoLink: true, defaultTarget: '_blank' }),
            new ListItemExtension({}),
            new MarkdownExtension({ htmlSanitizer: DOMPurify.sanitize, htmlToMarkdown, markdownToHtml }),
            new OrderedListExtension(),
            new UnderlineExtension(),
            new StrikeExtension(),
            new TableExtension({ resizable: false }),
            ...(readOnly ? [] : [new HistoryExtension({})]),
        ],
        content,
        stringHandler: 'markdown',
    });

    useImperativeHandle(ref, () => getContext(), [getContext]);

    useMount(() => {
        if (!props.doNotFocus) {
            manager.view.focus();
        }
    });

    // We need to track the modified content that we expect to be in the editor.
    // This way, if the content prop changes, we can update the editor content to match
    // if needed. However, we don't want to update the editor content on normal typing
    // changes because that would cause the cursor to jump around unexpectedly.
    const [modifiedContent, setModifiedContent] = useState(content);
    useEffect(() => {
        if (readOnly && content !== undefined) {
            manager.store.commands.setContent(content);
        } else if (!readOnly && content !== undefined && modifiedContent !== content) {
            // If we get a content change that doesn't match what we're tracking to be in the editor,
            // then we need to update the editor content to match the new props content.
            manager.store.commands.setContent(content);
            setModifiedContent(content);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [readOnly, content]);

    return (
        <EditorContainer className={className} onKeyDown={onKeyDown} data-testid={dataTestId} editorStyle={editorStyle}>
            <ThemeProvider theme={EditorTheme}>
                <Remirror classNames={['ant-typography']} editable={!readOnly} manager={manager} initialContent={state}>
                    {!readOnly && (
                        <>
                            <Toolbar />
                            <CodeBlockToolbar />
                            <FloatingToolbar />
                            <TableComponents tableCellMenuProps={{ Component: TableCellMenu }} />
                            <MentionsComponent />
                            {onChange && (
                                <OnChangeMarkdown
                                    onChange={(md: string) => {
                                        setModifiedContent(md);
                                        onChange(md);
                                    }}
                                />
                            )}
                        </>
                    )}
                    <EditorComponent />
                </Remirror>
            </ThemeProvider>
        </EditorContainer>
    );
});
