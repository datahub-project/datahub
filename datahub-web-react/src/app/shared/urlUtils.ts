const SAFE_URL_SCHEMES = new Set(['http:', 'https:', 'ftp:', 'mailto:']);

/**
 * Returns the URL unchanged if its scheme is in the safe allowlist (http, https, ftp, mailto).
 * Returns 'about:blank' for dangerous pseudo-schemes (javascript:, data:, vbscript:, etc.)
 * to prevent stored XSS when persisted URLs are rendered as <a href> attributes.
 *
 * Relative URLs (no scheme) are returned unchanged — they inherit the page's scheme.
 */
export function safeUrl(url: string | null | undefined): string {
    if (!url) return '';
    try {
        const { protocol } = new URL(url);
        return SAFE_URL_SCHEMES.has(protocol) ? url : 'about:blank';
    } catch {
        // URL constructor throws for relative paths ("/some/path", "../foo") — those are safe.
        return url;
    }
}
