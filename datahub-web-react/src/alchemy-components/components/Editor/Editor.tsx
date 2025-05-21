import { EditorComponent, Remirror, TableComponents, ThemeProvider, useRemirror } from '@remirror/react';
import DOMPurify from 'dompurify';
import React, { forwardRef, useEffect, useImperativeHandle } from 'react';
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

import { EditorContainer, EditorTheme } from '@components/components/Editor/EditorTheme';
import { OnChangeMarkdown } from '@components/components/Editor/OnChangeMarkdown';
import { htmlToMarkdown } from '@components/components/Editor/extensions/htmlToMarkdown';
import { markdownToHtml } from '@components/components/Editor/extensions/markdownToHtml';
import { DataHubMentionsExtension } from '@components/components/Editor/extensions/mentions/DataHubMentionsExtension';
import { MentionsComponent } from '@components/components/Editor/extensions/mentions/MentionsComponent';
import { CodeBlockToolbar } from '@components/components/Editor/toolbar/CodeBlockToolbar';
import { FloatingToolbar } from '@components/components/Editor/toolbar/FloatingToolbar';
import { TableCellMenu } from '@components/components/Editor/toolbar/TableCellMenu';
import { Toolbar } from '@components/components/Editor/toolbar/Toolbar';

type EditorProps = {
    readOnly?: boolean;
    content?: string;
    onChange?: (md: string) => void;
    className?: string;
    doNotFocus?: boolean;
    placeholder?: string;
};

export const Editor = forwardRef((props: EditorProps, ref) => {
    const { content, readOnly, onChange, className, placeholder } = props;
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
    useEffect(() => {
        if (readOnly && content) {
            manager.store.commands.setContent(content);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [readOnly, content]);

    return (
        <EditorContainer className={className}>
            <ThemeProvider theme={EditorTheme}>
                <Remirror
                    classNames={['ant-typography']}
                    editable={!readOnly}
                    manager={manager}
                    initialContent={state}
                    placeholder={placeholder || ''}
                >
                    {!readOnly && (
                        <>
                            <Toolbar />
                            <CodeBlockToolbar />
                            <FloatingToolbar />
                            <TableComponents tableCellMenuProps={{ Component: TableCellMenu }} />
                            <MentionsComponent />
                            {onChange && <OnChangeMarkdown onChange={onChange} />}
                        </>
                    )}
                    <EditorComponent />
                </Remirror>
            </ThemeProvider>
        </EditorContainer>
    );
});
