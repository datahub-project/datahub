/**
 * Where the Phase 2 (full metadata: descriptions, tags, terms, structured properties) load
 * stands, as one value so a cell renderer cannot see "loading" and "error" at once.
 */
export type MetadataStatus = 'loading' | 'error' | 'ready';

export function toMetadataStatus(loading: boolean, error: unknown): MetadataStatus {
    if (loading) return 'loading';
    if (error) return 'error';
    return 'ready';
}
