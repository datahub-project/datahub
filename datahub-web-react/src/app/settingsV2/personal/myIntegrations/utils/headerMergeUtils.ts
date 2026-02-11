/**
 * Utilities for merging custom headers in plugin configurations.
 */

/**
 * Represents a header entry that may come from existing user config (nullable key/value).
 * Matches the GraphQL StringMapEntry type shape.
 */
type ExistingHeaderEntry = {
    key?: string | null;
    value?: string | null;
};

/**
 * Merges new headers into existing headers, with new headers taking precedence.
 * Existing headers with the same key are overwritten by the new values.
 *
 * @param existingHeaders - Current headers on the plugin (may have nullable keys)
 * @param newHeaders - Headers to merge in (takes precedence over existing)
 * @returns Merged array of headers with non-null keys
 */
export function mergeCustomHeaders(
    existingHeaders: ExistingHeaderEntry[],
    newHeaders: { key: string; value: string }[],
): { key: string; value: string }[] {
    const newKeys = new Set(newHeaders.map((h) => h.key));
    const kept = existingHeaders
        .filter((h) => !newKeys.has(h.key || ''))
        .map((h) => ({ key: h.key || '', value: h.value || '' }));

    return [...kept, ...newHeaders];
}
