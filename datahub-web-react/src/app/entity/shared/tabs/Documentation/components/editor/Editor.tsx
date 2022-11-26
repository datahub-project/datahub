import React, { forwardRef, useImperativeHandle } from 'react';
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
    readonly?: boolean;
    content?: string;
    onChange?: (md: string) => void;
};

export const Editor = forwardRef((props: EditorProps, ref) => {
    const { content, readonly, onChange } = props;
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
            new ImageExtension({ enableResizing: !readonly }),
            new ItalicExtension(),
            new LinkExtension({ autoLink: true }),
            new ListItemExtension(),
            new MarkdownExtension({ htmlSanitizer: DOMPurify.sanitize, htmlToMarkdown, markdownToHtml }),
            new OrderedListExtension(),
            new UnderlineExtension(),
            new StrikeExtension(),
            new TableExtension({ resizable: false }),
            ...(readonly ? [] : [new HistoryExtension()]),
        ],
        content,
        stringHandler: 'markdown',
    });

    useImperativeHandle(ref, () => getContext(), [getContext]);
    useMount(() => {
        manager.view.focus();
    });

    return (
        <EditorContainer>
            <ThemeProvider theme={EditorTheme}>
                <Remirror classNames={['ant-typography']} editable={!readonly} manager={manager} initialContent={state}>
                    {!readonly && (
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
