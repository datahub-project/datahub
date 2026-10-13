import { css } from 'styled-components';

/** Content surface used by the read-only HTML renderer. Not a ProseMirror node. */
export const EDITOR_CONTENT_CLASS = 'datahub-editor-content';

type ContentStyleProps = {
    $readOnly?: boolean;
    $compact?: boolean;
};

/**
 * Typography shared by the editable ProseMirror surface and the read-only HTML
 * surface. Read-only rendering must not import Remirror, so these rules live here.
 */
export const editorContentCss = css<ContentStyleProps>`
    flex: 1 1 100%;
    border: 0;
    font-size: 14px;
    /* Editable editors need inset from the border; read-only viewers (sidebar,
     * search cards, CompactMarkdownViewer) should sit flush with surrounding text. */
    padding: ${(props) => {
        if (props.$compact) return '12px 16px 0 16px';
        if (props.$readOnly) return '0';
        return '16px';
    }};
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

    /*
     * The prism syntax theme paints its own code block background — a light
     * grey in light mode, a neutral dark grey in dark mode — neither of which
     * matches our surface. Only the token colors come from prism; the frame
     * comes from our tokens.
     */
    pre {
        background: ${(props) => props.theme.colors.bgSurface};
        border: 1px solid ${(props) => props.theme.colors.border};
        border-radius: 8px;
        padding: 12px;
        overflow-x: auto;
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

        /* Code blocks inside an expanded details section need to be inset
           from the section's own padding; the rest of the frame is shared. */
        pre {
            margin: 12px 16px 16px;
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
`;

/**
 * Markdown structures that Remirror's extension stylesheet paints in edit mode.
 * Applied only to the read-only surface, which does not load that stylesheet.
 */
export const readOnlyMarkdownCss = css`
    /* Marked HTML has newlines between blocks. pre-wrap (needed by ProseMirror)
       would paint those as extra gaps. */
    white-space: normal;

    p {
        margin: 0 0 0.75em;
    }

    p:last-child {
        margin-bottom: 0;
    }

    h1,
    h2,
    h3,
    h4,
    h5,
    h6 {
        margin: 0.6em 0 0.3em;
        line-height: 1.3;
    }

    ul,
    ol {
        margin: 0.5em 0;
        padding-left: 1.5em;
    }

    blockquote {
        margin: 0.5em 0;
        padding-left: 12px;
        border-left: 3px solid ${(props) => props.theme.colors.border};
    }

    :not(pre) > code {
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        background: ${(props) => props.theme.colors.bgSurface};
        border-radius: 4px;
        padding: 0.1em 0.35em;
    }

    pre code {
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }

    th,
    td {
        border: 1px solid ${(props) => props.theme.colors.border};
    }

    .mentions {
        font-weight: 500;
        padding: 0 4px;
        border-radius: 4px;
        background: ${(props) => props.theme.colors.bgHover};
    }
`;
