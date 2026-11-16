import {
    extensionBlockquoteStyledCss,
    extensionCalloutStyledCss,
    extensionCodeBlockStyledCss,
    extensionCountStyledCss,
    extensionGapCursorStyledCss,
    extensionImageStyledCss,
    extensionListStyledCss,
    extensionMentionAtomStyledCss,
    extensionPlaceholderStyledCss,
    extensionPositionerStyledCss,
    extensionTablesStyledCss,
} from '@remirror/styles/styled-components';
import { defaultRemirrorTheme } from '@remirror/theme';
import type { RemirrorThemeType } from '@remirror/theme';
import styled from 'styled-components';
import type { DefaultTheme } from 'styled-components';

import { EDITOR_CONTENT_CLASS, editorContentCss } from '@components/components/Editor/editorContentStyles';

export const getEditorTheme = (theme: DefaultTheme): RemirrorThemeType => ({
    ...defaultRemirrorTheme,
    fontSize: {
        default: '14px',
    },
    color: {
        border: 'none',
        outline: 'none',
        primary: theme.colors.textSuccess,
        table: {
            ...defaultRemirrorTheme.color.table,
            mark: theme.colors.textDisabled,
            default: {
                controller: theme.colors.bgHover,
                border: theme.colors.border,
            },
            selected: {
                controller: theme.colors.bgHover,
                border: theme.colors.border,
                cell: theme.colors.bgSurface,
            },
            preselect: {
                controller: theme.colors.borderDisabled,
                border: theme.colors.border,
            },
        },
    },
});

export const EditorContainer = styled.div<{
    $readOnly?: boolean;
    $hideBorder?: boolean;
    $fixedBottomToolbar?: boolean;
    $compact?: boolean;
}>`
    ${extensionBlockquoteStyledCss}
    ${extensionCalloutStyledCss}
    ${extensionCodeBlockStyledCss}
    ${extensionCountStyledCss}
    ${extensionGapCursorStyledCss}
    ${extensionImageStyledCss}
    ${extensionListStyledCss}
    ${extensionMentionAtomStyledCss}
    ${extensionPlaceholderStyledCss}
    ${extensionPositionerStyledCss}
    ${extensionTablesStyledCss}

    font-weight: 400;
    display: flex;
    flex: 1 1 auto;
    border: ${(props) => (props.$readOnly || props.$hideBorder ? `none` : `1px solid ${props.theme.colors.border}`)};
    border-radius: 12px;
    padding-bottom: ${(props) => (props.$fixedBottomToolbar ? '100px' : '0')};

    .remirror-theme,
    .remirror-editor-wrapper {
        flex: 1 1 100%;
        display: flex;
        flex-direction: column;
        max-width: 100%;
    }

    .remirror-editor.ProseMirror,
    .${EDITOR_CONTENT_CLASS} {
        ${editorContentCss}
    }

    .remirror-floating-popover {
        z-index: 100;
    }

    .remirror-is-empty::before {
        font-style: normal !important;
    }
`;
