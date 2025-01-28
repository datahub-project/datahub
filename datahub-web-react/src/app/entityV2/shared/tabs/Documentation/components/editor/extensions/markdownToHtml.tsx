import { marked } from 'marked';
import { DATAHUB_MENTION_ATTRS } from './mentions/DataHubMentionsExtension';

marked.use({
    renderer: {
        link(href, _, text) {
            /* Checks if the markdown link is of a DataHub mention format and
               parses it into the necessary DOM structure described in DataHubMentionsExtension */
            if (text.startsWith('@') && href?.startsWith('urn')) {
                return `<span ${DATAHUB_MENTION_ATTRS.urn}="${href}">${text}</span>`;
            }

            /* Returning false allows marked to use the default link parser */
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
