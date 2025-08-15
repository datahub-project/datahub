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

import { EditorContainer, EditorTheme } from '@app/entityV2/shared/tabs/Documentation/components/editor/EditorTheme';
import { OnChangeMarkdown } from '@app/entityV2/shared/tabs/Documentation/components/editor/OnChangeMarkdown';
import { htmlToMarkdown } from '@app/entityV2/shared/tabs/Documentation/components/editor/extensions/htmlToMarkdown';
import { markdownToHtml } from '@app/entityV2/shared/tabs/Documentation/components/editor/extensions/markdownToHtml';
import { DataHubMentionsExtension } from '@app/entityV2/shared/tabs/Documentation/components/editor/extensions/mentions/DataHubMentionsExtension';
import { MentionsComponent } from '@app/entityV2/shared/tabs/Documentation/components/editor/extensions/mentions/MentionsComponent';
import { CodeBlockToolbar } from '@app/entityV2/shared/tabs/Documentation/components/editor/toolbar/CodeBlockToolbar';
import { FloatingToolbar } from '@app/entityV2/shared/tabs/Documentation/components/editor/toolbar/FloatingToolbar';
import { TableCellMenu } from '@app/entityV2/shared/tabs/Documentation/components/editor/toolbar/TableCellMenu';
import { Toolbar } from '@app/entityV2/shared/tabs/Documentation/components/editor/toolbar/Toolbar';

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
