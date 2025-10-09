import { marked } from 'marked';

import { DATAHUB_MENTION_ATTRS } from '@components/components/Editor/extensions/mentions/DataHubMentionsExtension';
import { FILE_ATTRS } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';

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
    // Preprocess markdown to convert our custom file syntax to HTML
    const processedMarkdown = markdown.replace(
        /\[FILE:([^|]+)\|([^|]+)\|([^|]+)\|([^\]]+)\]/g,
        (match, name, type, size, url) => {
            // Convert our file syntax to a div that matches our parseDOM rule
            return `<div class="file-node" ${FILE_ATTRS.name}="${name}" ${FILE_ATTRS.type}="${type}" ${FILE_ATTRS.size}="${size}" ${FILE_ATTRS.url}="${url}"></div>`;
        }
    );

    return marked(processedMarkdown, {
        gfm: true,
        smartLists: true,
        xhtml: true,
        sanitizer,
        breaks: true,
    });
}
