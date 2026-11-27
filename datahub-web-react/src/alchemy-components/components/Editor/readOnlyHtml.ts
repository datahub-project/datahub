import DOMPurify from 'dompurify';

import { FILE_ATTRS } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { markdownToHtml } from '@components/components/Editor/extensions/markdownToHtml';
import { DATAHUB_MENTION_ATTRS } from '@components/components/Editor/extensions/mentions/constants';

const ALLOWED_SCHEMES = new Set(['http', 'https', 'mailto']);

// Raster screenshots pasted into descriptions before hosted uploads existed.
// SVG is excluded: a data:image/svg+xml document can carry script.
const DATA_IMAGE_URI = /^data:image\/(?:png|jpe?g|gif|webp|bmp);base64,[a-z0-9+/]+={0,2}$/i;

// Per image, after whitespace is removed. About 1.5 MB of decoded pixels.
export const MAX_DATA_IMAGE_URI_LENGTH = 2_000_000;

// Attributes the browser fetches or navigates. `srcset` is parsed separately.
// `background` still loads an image on table cells. `xlink:href` loads on SVG images.
export const URL_ATTRIBUTES = ['href', 'src', 'poster', 'action', 'formaction', 'background', 'xlink:href'] as const;

const XLINK_NAMESPACE = 'http://www.w3.org/1999/xlink';

/**
 * Browsers treat `\` as `/`, and strip tab, newline, and carriage return before
 * parsing. `/\host` and `/<tab>/host` are both protocol-relative `//host`.
 * The normalized form is only for the safety check. A passing value is left
 * as authored so a backslash in a query string is not rewritten to a slash.
 */
export function normalizeHref(href: string): string {
    return href
        .trim()
        .replace(/\\/g, '/')
        .replace(/[\t\n\r]/g, '');
}

/**
 * Allow http(s), mailto, and any relative reference that does not become a
 * protocol-relative host after normalization. Reject `javascript:`, `data:`,
 * and `//host`. Image `src` uses {@link isSafeImageSource} instead.
 */
export function isSafeHref(href: string): boolean {
    const normalized = normalizeHref(href);
    if (!normalized || normalized.startsWith('//')) return false;

    const scheme = /^([a-z][a-z0-9+.-]*):/i.exec(normalized);
    if (!scheme) return true;
    return ALLOWED_SCHEMES.has(scheme[1].toLowerCase());
}

/** `data:image` raster URIs are safe on `<img>` only, and only under the size cap. */
export function isSafeImageSource(value: string): boolean {
    if (isSafeHref(value)) return true;
    const normalized = normalizeHref(value).replace(/ /g, '');
    if (normalized.length > MAX_DATA_IMAGE_URI_LENGTH) return false;
    return DATA_IMAGE_URI.test(normalized);
}

export function scrubSrcset(value: string, isSafe: (url: string) => boolean = isSafeHref): string | null {
    const candidates = value
        .split(',')
        .map((candidate) => candidate.trim())
        .filter(Boolean);
    const safe = candidates.filter((candidate) => isSafe(candidate.split(/\s+/)[0] ?? ''));
    if (safe.length === 0) return null;
    if (safe.length === candidates.length) return value;
    return safe.join(', ');
}

function scrubXlinkHref(element: Element): void {
    const value = element.getAttributeNS(XLINK_NAMESPACE, 'href') ?? element.getAttribute('xlink:href');
    if (value === null || isSafeHref(value)) return;
    element.removeAttributeNS(XLINK_NAMESPACE, 'href');
    element.removeAttribute('xlink:href');
}

function attributeIsSafe(element: Element, name: string, value: string): boolean {
    if (element.tagName === 'IMG' && (name === 'src' || name === 'srcset')) return isSafeImageSource(value);
    return isSafeHref(value);
}

function scrubUrlAttributes(element: Element): void {
    const imageSource = element.tagName === 'IMG';
    URL_ATTRIBUTES.forEach((name) => {
        if (!element.hasAttribute(name)) return;
        const value = element.getAttribute(name) ?? '';
        if (!attributeIsSafe(element, name, value)) element.removeAttribute(name);
    });
    scrubXlinkHref(element);

    if (!element.hasAttribute('srcset')) return;
    const srcset = scrubSrcset(element.getAttribute('srcset') ?? '', imageSource ? isSafeImageSource : isSafeHref);
    if (srcset === null) element.removeAttribute('srcset');
    else element.setAttribute('srcset', srcset);
}

/**
 * Markdown to sanitized HTML for the read-only surface. Mention and file spans stay
 * in the HTML so the read-only view can mount the host's mention view and a file
 * card for them.
 */
export function toReadOnlyHtml(markdown: string): string {
    // DOMPurify blocks data: URIs unless the tag is listed. Only images may carry one.
    const sanitized = markdownToHtml(markdown, (html) => DOMPurify.sanitize(html, { ADD_DATA_URI_TAGS: ['img'] }));
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

    doc.querySelectorAll('*').forEach(scrubUrlAttributes);

    doc.querySelectorAll('a[href]').forEach((anchor) => {
        anchor.setAttribute('target', '_blank');
        anchor.setAttribute('rel', 'noopener noreferrer');
    });

    return doc.body.innerHTML;
}
