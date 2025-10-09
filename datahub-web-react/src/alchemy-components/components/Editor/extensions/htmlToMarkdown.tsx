import { ErrorConstant, defaultImport, invariant, isElementDomNode } from '@remirror/core';
import _TurndownService from 'turndown';
import { gfm } from 'turndown-plugin-gfm';

import { FILE_ATTRS } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { DATAHUB_MENTION_ATTRS } from '@components/components/Editor/extensions/mentions/DataHubMentionsExtension';
import { ptToPx } from '@components/components/Editor/utils';

const TurndownService = defaultImport(_TurndownService);

/**
 * Extracts file attributes from a DOM node with fallback to data- attributes
 * @param node - the DOM node containing file attributes
 * @returns object with file attributes
 */
function extractFileAttributes(node: HTMLElement) {
    return {
        url: node.getAttribute(FILE_ATTRS.url) || node.getAttribute('data-file-url') || '',
        name: node.getAttribute(FILE_ATTRS.name) || node.getAttribute('data-file-name') || '',
        type: node.getAttribute(FILE_ATTRS.type) || node.getAttribute('data-file-type') || '',
        size: node.getAttribute(FILE_ATTRS.size) || node.getAttribute('data-file-size') || '0',
    };
}

/**
 * Checks if the input HTML table could be parsed into a markdown table
 * @param element - the HTML table element
 * @returns true if the table is a valid markdown table, false otherwise
 */
function isValidMarkdownTable(element: HTMLElement): boolean {
    const invalidTags = ['ul', 'li', 'pre', 'table', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'];

    return !invalidTags.some((tag) => element.getElementsByTagName(tag).length > 0);
}

const br = '<br />';
const turndownService = new TurndownService({
    codeBlockStyle: 'fenced',
    headingStyle: 'atx',
    blankReplacement(_, node: any) {
        /** Replaces blank nodes in table to <br> to preserve line breaks */
        if (node.closest('table')) {
            return br;
        }
        /** Replaces all empty <p> tags to &nbsp; to preserve line breaks */
        if (node.nodeName === 'P') {
            return `\n\n&nbsp;\n\n`;
        }
        return node.isBlock ? '\n\n' : '';
    },
})
    /* Adds GFM support */
    .use(gfm)
    /* Keep HTML format if table does not have a valid markdown structure */
    .addRule('skipTable', {
        filter: (node) => node.nodeName === 'TABLE' && !isValidMarkdownTable(node),
        replacement: (_, node: any) => `${node.outerHTML}`,
    })
    /* Keep <br> tags in tables allowing line break within cells */
    .addRule('keepBreaksinTables', {
        filter: (node) => node.nodeName === 'BR' && !!node.closest('table'),
        replacement: () => br,
    })
    /* Remove unnecessary child <p> tags within tables <th> and <td> */
    .addRule('tableHeadingAndData', {
        filter: (node) =>
            !!(node.nodeName === 'P' && (node.parentNode?.nodeName === 'TH' || node.parentNode?.nodeName === 'TD')),
        replacement: (content, node: any) =>
            node?.previousElementSibling?.nodeName === 'P' ? `${br}${content}` : content,
    })
    /* Keep HTML format if image has an explicit width attribute */
    .addRule('images', {
        filter: (node) => node.nodeName === 'IMG' && node.hasAttribute('width'),
        replacement: (_, node: any) => `${node.outerHTML}`,
    })
    /* Add improved code block support from html (snippet from Remirror). */
    .addRule('fencedCodeBlock', {
        filter: (node, options) => {
            return !!(
                options.codeBlockStyle === 'fenced' &&
                node.nodeName === 'PRE' &&
                node.firstChild &&
                node.firstChild.nodeName === 'CODE'
            );
        },

        replacement: (_, node, options) => {
            invariant(isElementDomNode(node.firstChild), {
                code: ErrorConstant.EXTENSION,
                message: `Invalid node \`${node.firstChild?.nodeName}\` encountered for codeblock when converting html to markdown.`,
            });

            const className = node.firstChild.getAttribute('class') ?? '';
            const language =
                className.match(/(?:lang|language)-(\S+)/)?.[1] ??
                node.firstChild.getAttribute('data-code-block-language') ??
                '';

            return `\n\n${options.fence}${language}\n${node.firstChild.textContent}\n${options.fence}\n\n`;
        },
    })
    /* Formats HTML Datahub mention nodes to Markdown */
    .addRule('mentions', {
        filter: (node) => {
            return node.hasAttribute(DATAHUB_MENTION_ATTRS.urn);
        },
        replacement: (_, node) => {
            invariant(isElementDomNode(node), {
                code: ErrorConstant.EXTENSION,
                message: `Invalid node \`${node.nodeName}\` encountered for mentions when converting html to markdown.`,
            });
            const urn = node.getAttribute(DATAHUB_MENTION_ATTRS.urn);
            if (!urn) return '';

            return `[${node.textContent}](${urn})`;
        },
    })
    /* Formats HTML file nodes to Markdown - looks for React components with file-node class */
    .addRule('fileNodes', {
        filter: (node) => {
            // Check if node has file-node class or file-related attributes
            return (
                node.classList?.contains('file-node') ||
                (node.hasAttribute && (node.hasAttribute(FILE_ATTRS.name) || node.hasAttribute('data-file-name')))
            );
        },
        replacement: (_, node) => {
            invariant(isElementDomNode(node), {
                code: ErrorConstant.EXTENSION,
                message: `Invalid node \`${node.nodeName}\` encountered for file nodes when converting html to markdown.`,
            });

            const { url, name, type } = extractFileAttributes(node);

            // Check if this is an image file
            if (type.startsWith('image/')) {
                // Create standard markdown image syntax: ![filename](url)
                return `![${name}](${url})`;
            }
            // Create standard markdown link syntax: [filename](url)
            return `[${name}](${url})`;
        },
    })
    /* Add support for underline */
    .addRule('underline', {
        filter: (node) => {
            const nodeName = node.nodeName?.toUpperCase();
            return (
                nodeName === 'U' ||
                (nodeName === 'SPAN' &&
                    node instanceof HTMLElement &&
                    typeof node.style.textDecoration === 'string' &&
                    node.style.textDecoration.toLowerCase().includes('underline'))
            );
        },
        replacement: (content) => `<u>${content}</u>`,
    })
    /* Add support for handling font size change */
    .addRule('fontSize', {
        filter: (node) =>
            node instanceof HTMLElement && node.nodeName?.toUpperCase() === 'SPAN' && !!node.style.fontSize,
        replacement: (content, node) => {
            const elem = node as HTMLElement;
            let size = elem.style.fontSize.trim();
            if (!size) return content;

            // Convert pt to px
            if (size.endsWith('pt')) {
                const pts = parseFloat(size);
                size = `${ptToPx(pts)}px`;
            }

            return `<span style="font-size:${size}">${content}</span>`;
        },
    });

/**
 * Converts the provided HTML to markdown.
 * @param html - a html string of the content
 * @returns parsed markdown string of the html content
 */
export function htmlToMarkdown(html: string): string {
    const result = turndownService.turndown(html);
    return result === br ? '' : result;
}
