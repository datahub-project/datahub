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

export const EditorTheme: RemirrorThemeType = {
    ...defaultRemirrorTheme,
    fontSize: {
        default: '14px',
    },
    color: {
        ...defaultRemirrorTheme.color,
        border: 'none',
        outline: 'none',
    },
};

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
    border: ${(props) => (props.$readOnly || props.$hideBorder ? 'none' : `1px solid ${props.theme.colors.border}`)};
    border-radius: 12px;
    padding-bottom: ${(props) => (props.$fixedBottomToolbar ? '100px' : '0')};

    --rmr-color-primary: ${(props) => props.theme.colors.textBrand};
    --rmr-color-table-mark: ${(props) => props.theme.colors.border};
    --rmr-color-table-default-controller: ${(props) => props.theme.colors.bgSurface};
    --rmr-color-table-default-border: ${(props) => props.theme.colors.border};
    --rmr-color-table-selected-controller: ${(props) => props.theme.colors.bgHover};
    --rmr-color-table-selected-border: ${(props) => props.theme.colors.border};
    --rmr-color-table-selected-cell: ${(props) => props.theme.colors.bg};
    --rmr-color-table-preselect-controller: ${(props) => props.theme.colors.bgHover};
    --rmr-color-table-preselect-border: ${(props) => props.theme.colors.border};

    .remirror-theme,
    .remirror-editor-wrapper {
        flex: 1 1 100%;
        display: flex;
        flex-direction: column;
        max-width: 100%;
    }

    .remirror-editor.ProseMirror {
        flex: 1 1 100%;
        border: 0;
        font-size: 14px;
        padding: ${(props) => (props.$compact ? '12px 16px 0 16px' : '16px')};
        position: relative;
        outline: 0;
        line-height: ${(props) => (props.$compact ? '20px' : '1.5')};
        white-space: pre-wrap;
        margin: 0;
        color: ${(props) => props.theme.colors.text} !important;
        min-height: ${(props) => (props.$compact ? '80px' : 'auto')};
        max-height: ${(props) => (props.$compact ? '80px' : 'auto')};
        overflow-y: ${(props) => (props.$compact ? 'auto' : 'visible')};

        a {
            font-weight: 500;
            color: ${(props) => props.theme.colors.textBrand};
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
            background: ${(props) => props.theme.colors.bgHover};
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
                min-width: 120px;
            }
        }

        /* Scrollbar styling (only visible when overflow is auto, i.e. compact mode) */
        &::-webkit-scrollbar {
            width: 4px;
        }

        &::-webkit-scrollbar-thumb {
            background-color: ${(props) => props.theme.colors.scrollbarThumb};
            border-radius: 2px;
        }
    }

    .remirror-floating-popover {
        z-index: 100;
    }

    .remirror-is-empty::before {
        font-style: normal !important;
    }
`;
