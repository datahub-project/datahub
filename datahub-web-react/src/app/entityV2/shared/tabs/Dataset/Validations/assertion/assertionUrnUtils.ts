export function isValidAssertionUrnFormat(urn: string | null | undefined): boolean {
    if (!urn) return false;
    if (!urn.startsWith('urn:li:assertion:')) return false;
    // Disallow query strings/fragments/whitespace. These indicate the URN is actually a URL-ish value.
    if (/[?#\s]/.test(urn)) return false;
    return true;
}
