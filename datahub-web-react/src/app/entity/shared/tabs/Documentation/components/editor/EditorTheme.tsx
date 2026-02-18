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
                border: theme.colors.textDisabled,
            },
        },
    },
});

/** @deprecated Use getEditorTheme(theme) instead */
export const EditorTheme: RemirrorThemeType = {
    ...defaultRemirrorTheme,
    fontSize: {
        default: '14px',
    },
    color: {
        border: 'none',
        outline: 'none',
        primary: '#00B14F',
        table: {
            ...defaultRemirrorTheme.color.table,
            mark: '#BFBFBF',
            default: {
                controller: '#F5F5F5',
                border: '#E9E9E9',
            },
            selected: {
                controller: '#F0F0F0',
                border: '#E9E9E9',
                cell: '#F8F8F8',
            },
            preselect: {
                controller: '#D9D9D9',
                border: '#BFBFBF',
            },
        },
    },
};

export const EditorContainer = styled.div<{ editorStyle?: string }>`
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

    .remirror-theme,
    .remirror-editor-wrapper {
        flex: 1 1 100%;
        display: flex;
        flex-direction: column;
    }

    .remirror-editor.ProseMirror {
        flex: 1 1 100%;
        border: 0;
        font-size: 14px;
        padding: 32px;
        position: relative;
        outline: 0;
        line-height: 1.5;
        white-space: pre-wrap;
        margin: 0;
        ${(props) => props.editorStyle}

        a {
            font-weight: 500;
        }

        li {
            ~ li {
                margin-top: 0.25em;
            }
            p {
                margin: 0;
            }
        }

        img {
            margin: 0.25em 0;
            &:not([width]) {
                max-width: 100%;
            }
        }

        hr {
            margin: 2rem 0;
            border-color: ${(props) => props.theme.colors.border};
        }

        .autocomplete {
            padding: 0.2rem;
            background: ${(props) => props.theme.colors.bgSurface};
            border-radius: 4px;
        }

        table {
            display: block;
            th:not(.remirror-table-controller) {
                background: ${(props) => props.theme.colors.bgSurface};
            }

            th:not(.remirror-table-controller),
            td {
                padding: 16px;
            }
        }
    }

    .remirror-floating-popover {
        z-index: 100;
    }
`;
