import { colors } from '@components';
import styled from 'styled-components';

import { ChatVariant } from '@app/chat/types';

/**
 * Markdown styling for chat messages.
 * Handles typography, code blocks, lists, tables, and other markdown elements.
 */
export const MarkdownContent = styled.div<{ isUser: boolean; $variant?: ChatVariant }>`
    font-size: 14px;
    line-height: 1.7;
    /* Base color for all text - inherited by all child elements */
    color: ${colors.gray[600]};
    overflow-wrap: anywhere; /* Break words anywhere if needed */
    word-break: break-word; /* Break long words */
    ${(props) =>
        props.$variant === ChatVariant.Compact &&
        `
        white-space: normal;
        width: 100%;
        max-width: 100%;
        min-width: 0;
        box-sizing: border-box;
    `}

    /* Override markdown renderer defaults in compact mode */
    ${(props) =>
        props.$variant === ChatVariant.Compact &&
        `
        & .wmde-markdown {
            white-space: normal !important;
            overflow-wrap: anywhere !important;
            word-break: break-word !important;
            max-width: 100% !important;
        }
    `}

    /* Style markdown output - apply to both direct children and MDEditor rendered content */
    & p,
    & .wmde-markdown p {
        margin: 0 0 8px 0;
        overflow-wrap: anywhere !important; /* Break words anywhere if needed */
        word-break: break-word !important; /* Break long words if necessary */
        ${(props) => props.$variant === ChatVariant.Compact && 'white-space: normal !important;'}
        font-weight: 400; /* Regular weight */
        font-size: 14px;
        max-width: 100%;
    }

    & p:last-child,
    & .wmde-markdown p:last-child {
        margin-bottom: 0;
    }

    /* Paragraphs containing strong tags (headers) should have no bottom margin.
       The AI wraps header text (e.g., "**Header Text**") in <p><strong> tags,
       so we target the parent <p> to remove its default bottom margin and maintain
       consistent spacing between headers and content. */
    & p:has(strong),
    & .wmde-markdown p:has(strong) {
        margin-bottom: 0;
    }

    & strong,
    & .wmde-markdown strong {
        font-weight: 600;
        font-size: inherit; /* Use same size as parent text for inline bold */
        margin-bottom: 0;
        /* Color inherited from parent */
    }

    & code,
    & .wmde-markdown code:not([class*='language-']) {
        background-color: ${colors.gray[1500]};
        padding: 2px 6px;
        border-radius: 4px;
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 13px;
        word-wrap: break-word;
        overflow-wrap: break-word;
    }

    /* Don't hide any pre elements - let MDEditor render what our regex doesn't catch */

    /* Override MDEditor's black code color to allow syntax highlighting */
    & .wmde-markdown code[class*='language-'],
    & .wmde-markdown pre[class*='language-'] {
        color: inherit !important; /* Allow syntax highlighter to set colors */
    }

    & .wmde-markdown code {
        color: inherit !important; /* Base code inherits gray[600] */
    }

    /* Override MDEditor's heading colors */
    & .wmde-markdown h1,
    & .wmde-markdown h2,
    & .wmde-markdown h3,
    & .wmde-markdown h4,
    & .wmde-markdown h5,
    & .wmde-markdown h6 {
        color: ${colors.gray[600]} !important;
    }

    /* Override MDEditor's link colors */
    & a,
    & .wmde-markdown a {
        color: ${colors.violet[600]} !important; /* Links use violet */
        text-decoration: underline;
        &:hover {
            text-decoration: underline;
        }
    }

    /* Ensure links inside headings are also violet */
    & h1 a,
    & h2 a,
    & h3 a,
    & h4 a,
    & h5 a,
    & h6 a,
    & .wmde-markdown h1 a,
    & .wmde-markdown h2 a,
    & .wmde-markdown h3 a,
    & .wmde-markdown h4 a,
    & .wmde-markdown h5 a,
    & .wmde-markdown h6 a {
        color: ${colors.violet[600]} !important;
        text-decoration: underline;
        &:hover {
            text-decoration: underline;
        }
    }

    & ul,
    & ol,
    & .wmde-markdown ul,
    & .wmde-markdown ol {
        margin: 0 0 8px 0;
        padding-left: 24px;
    }

    & li,
    & .wmde-markdown li {
        margin: 4px 0;
        font-weight: 400; /* Regular weight */
        font-size: 14px;
        color: ${colors.gray[600]}; /* Ensure list items are gray */
        overflow-wrap: anywhere;
        word-break: break-word;
        ${(props) => props.$variant === ChatVariant.Compact && 'white-space: normal !important;'}
        hyphens: auto;
    }

    & blockquote {
        border-left: 4px solid ${(props) => (props.isUser ? 'rgba(255, 255, 255, 0.5)' : colors.gray[100])};
        padding-left: 16px;
        margin: 8px 0;
        color: ${colors.gray[600]};
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
    }

    & h1,
    & h2,
    & h3,
    & h4,
    & h5,
    & h6 {
        margin: 32px 0 8px 0;
        font-weight: 600;
        color: ${colors.gray[600]} !important;
        overflow-wrap: anywhere;
        word-break: break-word;
        hyphens: auto;
    }

    /* Remove top margin from first heading */
    & h1:first-child,
    & h2:first-child,
    & h3:first-child,
    & h4:first-child,
    & h5:first-child,
    & h6:first-child {
        margin-top: 0;
    }

    & h1 {
        font-size: 20px;
    }

    & h2 {
        font-size: 18px;
    }

    & h3 {
        font-size: 16px;
    }

    /* Handle wide content like tables */
    & table {
        width: 100%;
        max-width: 100%;
        border-collapse: collapse;
        overflow-x: auto;
        display: block;
        white-space: nowrap;
    }

    & table thead,
    & table tbody,
    & table tr {
        display: table;
        width: 100%;
        table-layout: fixed;
    }

    & table td,
    & table th {
        word-wrap: break-word;
        overflow-wrap: break-word;
        max-width: 0;
    }

    /* Handle other wide elements */
    & img {
        max-width: 100%;
        height: auto;
    }
`;
