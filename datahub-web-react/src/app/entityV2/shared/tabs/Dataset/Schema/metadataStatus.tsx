import React from 'react';

import CellSkeleton from '@app/entityV2/shared/tabs/Dataset/Schema/components/CellSkeleton';
import MetadataUnavailable from '@app/entityV2/shared/tabs/Dataset/Schema/components/MetadataUnavailable';

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

/**
 * The one rendering of a Phase 2 metadata cell, shared by SchemaTable's description/tag/term
 * columns and the structured-property columns: skeleton while loading, an explicit
 * "unavailable" marker when the full query failed (a blank cell would read as "no tags"),
 * the real content otherwise.
 */
export function renderMetadataCell(
    status: MetadataStatus,
    width: number,
    content: () => React.ReactNode,
    skeletonTestId = 'metadata-cell-skeleton',
): React.ReactNode {
    if (status === 'loading') return <CellSkeleton $width={width} data-testid={skeletonTestId} />;
    if (status === 'error') return <MetadataUnavailable />;
    return content();
}
