import { marked } from 'marked';

import { FILE_ATTRS, isFileUrl } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { DATAHUB_MENTION_ATTRS } from '@components/components/Editor/extensions/mentions/DataHubMentionsExtension';

function escapeHtml(str: string): string {
    return str.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

marked.use({
    renderer: {
        // marked v16 passes a single link token ({ href, title, text, tokens }).
        link({ href, text }) {
            /* Checks if the markdown link is of a DataHub mention format and
               parses it into the necessary DOM structure described in DataHubMentionsExtension */
            if (text.startsWith('@') && href?.startsWith('urn')) {
                return `<span ${DATAHUB_MENTION_ATTRS.urn}="${escapeHtml(href)}">${escapeHtml(text)}</span>`;
            }

            /* Check if this is a file link (URL points to our file storage) */
            if (href && isFileUrl(href)) {
                return `<span class="file-node" ${FILE_ATTRS.url}="${escapeHtml(href)}" ${FILE_ATTRS.name}="${escapeHtml(text)}"></span>`;
            }

            /* Returning false allows marked to use the default link parser */
            return false;
        },
    },
});

export function markdownToHtml(markdown: string, sanitizer?: (html: string) => string): string {
    const html = marked(markdown, {
        gfm: true,
        breaks: true,
    }) as string;

    return sanitizer ? sanitizer(html) : html;
}
