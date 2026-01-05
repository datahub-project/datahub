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
    FontSizeExtension,
    GapCursorExtension,
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
import { FileDragDropExtension } from '@components/components/Editor/extensions/fileDragDrop/FileDragDropExtension';
import { htmlToMarkdown } from '@components/components/Editor/extensions/htmlToMarkdown';
import { markdownToHtml } from '@components/components/Editor/extensions/markdownToHtml';
import { DataHubMentionsExtension } from '@components/components/Editor/extensions/mentions/DataHubMentionsExtension';
import { MentionsComponent } from '@components/components/Editor/extensions/mentions/MentionsComponent';
import { CodeBlockToolbar } from '@components/components/Editor/toolbar/CodeBlockToolbar';
import { FloatingToolbar } from '@components/components/Editor/toolbar/FloatingToolbar';
import { TableCellMenu } from '@components/components/Editor/toolbar/TableCellMenu';
import { Toolbar } from '@components/components/Editor/toolbar/Toolbar';
import { EditorProps } from '@components/components/Editor/types';
import { colors } from '@components/theme';

import { notEmpty } from '@app/entityV2/shared/utils';

export const Editor = forwardRef((props: EditorProps, ref) => {
    const {
        content,
        readOnly,
        onChange,
        className,
        placeholder,
        hideHighlightToolbar,
        toolbarStyles,
        dataTestId,
        onKeyDown,
        hideBorder,
        uploadFileProps,
        fixedBottomToolbar,
    } = props;
    const { manager, state, getContext } = useRemirror({
        extensions: () => [
            new BlockquoteExtension(),
            new BoldExtension({}),
            new BulletListExtension({}),
            new CodeBlockExtension({ syntaxTheme: 'base16_ateliersulphurpool_light' }),
            new CodeExtension(),
            new DataHubMentionsExtension({}),
            new DropCursorExtension({
                color: colors.primary[100],
                width: 2,
            }),
            new HardBreakExtension(),
            new HeadingExtension({}),
            new HistoryExtension({}),
            new HorizontalRuleExtension({}),
            new FileDragDropExtension({
                uploadFileProps,
            }),
            new GapCursorExtension(), // required to allow cursor placement next to non-editable inline elements
            new ImageExtension({ enableResizing: !readOnly }),
            new ItalicExtension(),
            new LinkExtension({ autoLink: true, defaultTarget: '_blank' }),
            new ListItemExtension({}),
            new MarkdownExtension({ htmlSanitizer: DOMPurify.sanitize, htmlToMarkdown, markdownToHtml }),
            new OrderedListExtension(),
            new UnderlineExtension(),
            new StrikeExtension(),
            new TableExtension({ resizable: false }),
            new FontSizeExtension({}),
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
        if (readOnly && notEmpty(content)) {
            manager.store.commands.setContent(content);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [readOnly, content]);

    return (
        <EditorContainer
            className={className}
            data-testid={dataTestId}
            $readOnly={readOnly}
            onKeyDown={onKeyDown}
            $hideBorder={hideBorder}
            $fixedBottomToolbar={fixedBottomToolbar}
        >
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
                            <Toolbar styles={toolbarStyles} fixedBottom={fixedBottomToolbar} />
                            <CodeBlockToolbar />
                            {!hideHighlightToolbar && <FloatingToolbar />}
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
