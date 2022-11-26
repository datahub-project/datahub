import { marked } from 'marked';
import { DATAHUB_MENTION_ATTRS } from './mentions/DataHubMentionsExtension';

marked.use({
    renderer: {
        link(href, _, text) {
            if (text.startsWith('@') && href?.startsWith('urn')) {
                return `<span ${DATAHUB_MENTION_ATTRS.urn}="${href}">${text}</span>`;
            }
            return false;
        },
    },
});

export function markdownToHtml(markdown: string, sanitizer?: (html: string) => string): string {
    return marked(markdown, {
        gfm: true,
        smartLists: true,
        xhtml: true,
        sanitizer,
        breaks: true,
    });
}
