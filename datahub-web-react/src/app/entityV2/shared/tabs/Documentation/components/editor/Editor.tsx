import React, { forwardRef, useEffect, useImperativeHandle } from 'react';
import DOMPurify from 'dompurify';
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
import { EditorComponent, Remirror, useRemirror, ThemeProvider, TableComponents } from '@remirror/react';
import { useMount } from 'react-use';
import { EditorContainer, EditorTheme } from './EditorTheme';
import { htmlToMarkdown } from './extensions/htmlToMarkdown';
import { markdownToHtml } from './extensions/markdownToHtml';
import { CodeBlockToolbar } from './toolbar/CodeBlockToolbar';
import { FloatingToolbar } from './toolbar/FloatingToolbar';
import { Toolbar } from './toolbar/Toolbar';
import { OnChangeMarkdown } from './OnChangeMarkdown';
import { TableCellMenu } from './toolbar/TableCellMenu';
import { DataHubMentionsExtension } from './extensions/mentions/DataHubMentionsExtension';
import { MentionsComponent } from './extensions/mentions/MentionsComponent';

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
            new BoldExtension(),
            new BulletListExtension(),
            new CodeBlockExtension({ syntaxTheme: 'base16_ateliersulphurpool_light' }),
            new CodeExtension(),
            new DataHubMentionsExtension(),
            new DropCursorExtension(),
            new HardBreakExtension(),
            new HeadingExtension(),
            new HistoryExtension(),
            new HorizontalRuleExtension(),
            new ImageExtension({ enableResizing: !readOnly }),
            new ItalicExtension(),
            new LinkExtension({ autoLink: true, defaultTarget: '_blank' }),
            new ListItemExtension(),
            new MarkdownExtension({ htmlSanitizer: DOMPurify.sanitize, htmlToMarkdown, markdownToHtml }),
            new OrderedListExtension(),
            new UnderlineExtension(),
            new StrikeExtension(),
            new TableExtension({ resizable: false }),
            ...(readOnly ? [] : [new HistoryExtension()]),
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
