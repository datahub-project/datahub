import DOMPurify from 'dompurify';

import { FILE_ATTRS } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { markdownToHtml } from '@components/components/Editor/extensions/markdownToHtml';
import { DATAHUB_MENTION_ATTRS } from '@components/components/Editor/extensions/mentions/constants';

// Relative file URLs point at DataHub's file API. A single leading slash is a
// same-origin path; "//host" is protocol-relative and must not open in a new tab.
// Everything else must be an absolute http(s) or mailto link. DOMPurify has
// already dropped javascript: URLs.
const SAFE_HREF = /^(https?:|mailto:|\/(?!\/)|#)/i;

function isSafeHref(href: string): boolean {
    return SAFE_HREF.test(href.trim());
}

/**
 * Markdown to sanitized HTML for the read-only surface. Mention spans stay static
 * (no urn lookup). File spans stay in the HTML so the read-only view can mount a
 * file card for them.
 */
export function toReadOnlyHtml(markdown: string): string {
    const sanitized = markdownToHtml(markdown, DOMPurify.sanitize);
    if (typeof DOMParser === 'undefined') {
        return sanitized;
    }

    const doc = new DOMParser().parseFromString(sanitized, 'text/html');

    doc.querySelectorAll(`span[${DATAHUB_MENTION_ATTRS.urn}]`).forEach((node) => {
        node.classList.add('mentions');
    });

    doc.querySelectorAll('span.file-node').forEach((node) => {
        const url = node.getAttribute(FILE_ATTRS.url) ?? '';
        const name = node.getAttribute(FILE_ATTRS.name) || url;
        if (!isSafeHref(url)) {
            node.replaceWith(doc.createTextNode(name));
        }
    });

    doc.querySelectorAll('a[href]').forEach((anchor) => {
        const href = anchor.getAttribute('href') ?? '';
        if (!isSafeHref(href)) {
            anchor.removeAttribute('href');
            return;
        }
        anchor.setAttribute('target', '_blank');
        anchor.setAttribute('rel', 'noopener noreferrer');
    });

    return doc.body.innerHTML;
}
