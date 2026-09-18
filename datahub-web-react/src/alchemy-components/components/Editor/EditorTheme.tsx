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
        color: ${(props) => props.theme.colors.text};
        min-height: ${(props) => (props.$compact ? '80px' : 'auto')};
        max-height: ${(props) => (props.$compact ? '80px' : 'auto')};
        overflow-y: ${(props) => (props.$compact ? 'auto' : 'visible')};

        a {
            font-weight: 500;
            color: ${(props) => props.theme.colors.hyperlinks};
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
            border-color: ${(props) => props.theme.colors.overlayLight};
        }

        details {
            border: 1px solid ${(props) => props.theme.colors.border};
            border-radius: 12px;
            box-shadow: ${(props) => props.theme.colors.shadowXs};
            margin: 0.5em 0;
            overflow: hidden;
            summary {
                cursor: pointer;
                font-weight: 500;
                /* Extra right padding reserves space for the absolutely-positioned caret */
                padding: 12px 40px 12px 14px;
                user-select: none;
                list-style: none;
                position: relative;

                /* Remove the browser's native disclosure marker */
                &::-webkit-details-marker {
                    display: none;
                }

                /*
                 * CSS-only chevron — avoids data: URIs so it works under strict CSP
                 * (production blocks data: in mask-image; localhost:3000 does not enforce CSP).
                 * Two border sides of a rotated square form the down-pointing chevron;
                 * the open state rotates it 180° to point up.
                 */
                &::after {
                    content: '';
                    position: absolute;
                    right: 18px;
                    top: 50%;
                    width: 8px;
                    height: 8px;
                    border-right: 1px solid ${(props) => props.theme.colors.icon};
                    border-bottom: 1px solid ${(props) => props.theme.colors.icon};
                    transform: translateY(-75%) rotate(45deg);
                    transition: transform 0.2s ease;
                }
            }

            &[open] > summary {
                border-bottom: 1px solid ${(props) => props.theme.colors.border};

                &::after {
                    transform: translateY(-25%) rotate(-135deg);
                }
            }

            /* Code blocks inside an expanded details section */
            pre {
                background: ${(props) => props.theme.colors.bgSurface} !important;
                border: 1px solid ${(props) => props.theme.colors.border} !important;
                border-radius: 8px !important;
                margin: 12px 16px 16px !important;
                padding: 12px !important;
                overflow-x: auto;
            }
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
                min-width: 120px;
            }
        }

        /* Scrollbar styling (only visible when overflow is auto, i.e. compact mode) */
        &::-webkit-scrollbar {
            width: 4px;
        }

        &::-webkit-scrollbar-thumb {
            background-color: ${(props) => props.theme.colors.textDisabled};
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
