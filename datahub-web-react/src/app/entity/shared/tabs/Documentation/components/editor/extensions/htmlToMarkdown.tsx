import _TurndownService from 'turndown';
import { gfm } from 'turndown-plugin-gfm';
import { defaultImport, ErrorConstant, invariant, isElementDomNode } from '@remirror/core';
import { DATAHUB_MENTION_ATTRS } from './mentions/DataHubMentionsExtension';

const TurndownService = defaultImport(_TurndownService);

function isValidMarkdownTable(element: HTMLElement): boolean {
    let valid = true;

    const invalidTags = ['ul', 'li', 'pre', 'table', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'];
    invalidTags.forEach((tag) => {
        if (element.getElementsByTagName(tag).length > 0) {
            valid = false;
        }
    });

    return valid;
}

const br = '<br />';
const turndownService = new TurndownService({
    codeBlockStyle: 'fenced',
    headingStyle: 'atx',
    blankReplacement(_, node: any) {
        if (node.closest('table')) {
            return br;
        }
        if (node.nodeName === 'P') {
            return `\n\n&nbsp;\n\n`;
        }
        return node.isBlock ? '\n\n' : '';
    },
})
    .use(gfm)
    .addRule('skipTable', {
        filter: (node) => node.nodeName === 'TABLE' && !isValidMarkdownTable(node),
        replacement: (_, node: any) => `${node.outerHTML}`,
    })
    .addRule('keepBreaksinTables', {
        filter: (node) => node.nodeName === 'BR' && !!node.closest('table'),
        replacement: () => br,
    })
    .addRule('tableHeadingAndData', {
        filter: (node) =>
            !!(node.nodeName === 'P' && (node.parentNode?.nodeName === 'TH' || node.parentNode?.nodeName === 'TD')),
        replacement: (content, node: any) =>
            node?.previousElementSibling?.nodeName === 'P' ? `${br}${content}` : content,
    })
    .addRule('images', {
        filter: (node) => node.nodeName === 'IMG' && node.hasAttribute('width'),
        replacement: (_, node: any) => `${node.outerHTML}`,
    })
    // Add improved code block support from html.
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
    // (DataHub) Add mentions support
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
    });

/* Converts the provide HTML to markdown. */
export function htmlToMarkdown(html: string): string {
    const result = turndownService.turndown(html);
    return result === br ? '' : result;
}
