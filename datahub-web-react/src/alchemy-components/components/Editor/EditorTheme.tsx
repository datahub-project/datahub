import { defaultRemirrorTheme } from '@remirror/theme';
import type { RemirrorThemeType } from '@remirror/theme';
import styled from 'styled-components';

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
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';

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
            mark: ANTD_GRAY[6],
            default: {
                controller: ANTD_GRAY[3],
                border: ANTD_GRAY[4.5],
            },
            selected: {
                controller: ANTD_GRAY[4],
                border: ANTD_GRAY[4.5],
                cell: ANTD_GRAY[2.5],
            },
            preselect: {
                controller: ANTD_GRAY[5],
                border: ANTD_GRAY[6],
            },
        },
    },
};

export const EditorContainer = styled.div`
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
    border: 1px solid ${ANTD_GRAY[4.5]};
    border-radius: 12px;

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
        padding: 16px;
        position: relative;
        outline: 0;
        line-height: 1.5;
        white-space: pre-wrap;
        margin: 0;

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
            border-color: rgba(0, 0, 0, 0.06);
        }

        .autocomplete {
            padding: 0.2rem;
            background: ${ANTD_GRAY[4]};
            border-radius: 4px;
        }

        table {
            display: block;
            th:not(.remirror-table-controller) {
                background: ${ANTD_GRAY[2]};
            }

            th:not(.remirror-table-controller),
            td {
                padding: 16px;
                min-width: 120px;
            }
        }
    }

    .remirror-floating-popover {
        z-index: 100;
    }
`;
