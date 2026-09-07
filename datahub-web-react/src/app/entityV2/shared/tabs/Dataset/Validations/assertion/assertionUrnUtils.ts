export function isValidAssertionUrnFormat(urn: string | null | undefined): boolean {
    if (!urn) return false;
    if (!urn.startsWith('urn:li:assertion:')) return false;
    return !/[?#\s]/.test(urn);
}
