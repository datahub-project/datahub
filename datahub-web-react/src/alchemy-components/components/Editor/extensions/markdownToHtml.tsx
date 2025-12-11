/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { marked } from 'marked';

import { FILE_ATTRS, isFileUrl } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { DATAHUB_MENTION_ATTRS } from '@components/components/Editor/extensions/mentions/DataHubMentionsExtension';

marked.use({
    renderer: {
        link(href, _, text) {
            /* Checks if the markdown link is of a DataHub mention format and
               parses it into the necessary DOM structure described in DataHubMentionsExtension */
            if (text.startsWith('@') && href?.startsWith('urn')) {
                return `<span ${DATAHUB_MENTION_ATTRS.urn}="${href}">${text}</span>`;
            }

            /* Check if this is a file link (URL points to our file storage) */
            if (href && isFileUrl(href)) {
                return `<span class="file-node" ${FILE_ATTRS.url}="${href}" ${FILE_ATTRS.name}="${text}"></span>`;
            }

            /* Returning false allows marked to use the default link parser */
            return false;
        },
    },
});

export function markdownToHtml(markdown: string, sanitizer?: (html: string) => string): string {
    const html = marked(markdown, {
        gfm: true,
        smartLists: true,
        xhtml: true,
        breaks: true,
    }) as string;

    return sanitizer ? sanitizer(html) : html;
}
