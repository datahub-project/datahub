import { createBrowseV2SearchFilter } from '@app/searchV2/filters/utils';
import { applyFacetFilterOverrides } from '@app/searchV2/utils/applyFilterOverrides';
import { BROWSE_PATH_V2_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

import { EntityType, FacetFilterInput, FilterOperator } from '@types';

export const BROWSE_SIDEBAR_SEARCH_PAGE_SIZE = 50;
export const BROWSE_SIDEBAR_SEARCH_DEBOUNCE_MS = 250;

export type BrowseSearchPathEntry = {
    /** Indexed browsePathV2 id — container URN when the node is a container. */
    name: string;
    displayName?: string;
    entity?: { urn: string; type: EntityType } | null;
};

export type BrowseSearchEntity = {
    urn: string;
    type: EntityType;
    name: string;
    platform?: { urn: string; name: string } | null;
    origin?: string | null;
    browsePath: BrowseSearchPathEntry[];
};

export type BrowseSearchHitKind = 'platform' | 'path' | 'entity';

export type BrowseSearchHit = {
    key: string;
    kind: BrowseSearchHitKind;
    label: string;
    platformUrn?: string;
    platformName?: string;
    origin?: string | null;
    path: string[];
    /** Display names for `path` (never URNs). Same length as `path`. */
    pathLabels?: string[];
    entity?: { urn: string; type: EntityType } | null;
};

function displayPathLabel(entry: BrowseSearchPathEntry): string {
    const label = entry.displayName || entry.name;
    return label.startsWith('urn:li:') ? '' : label;
}

const KIND_ORDER: Record<BrowseSearchHitKind, number> = {
    platform: 0,
    path: 1,
    entity: 2,
};

export function browseSearchHitLocation(hit: Pick<BrowseSearchHit, 'kind' | 'platformName' | 'pathLabels'>): string {
    const labels = (hit.pathLabels ?? []).filter(Boolean);
    let ancestors: string[] = [];
    if (hit.kind === 'path') {
        ancestors = labels.slice(0, -1);
    } else if (hit.kind === 'entity') {
        ancestors = labels;
    }
    const parts: string[] = [];
    // Folder rows use a platform icon; entity rows need the platform named in the location.
    if (hit.kind === 'entity' && hit.platformName) {
        parts.push(hit.platformName);
    }
    parts.push(...ancestors);
    return parts.join(' / ');
}

export function isBrowseSidebarSearchActive(searchInput: string): boolean {
    return searchInput.trim().length > 0;
}

export function nameMatchesQuery(name: string | null | undefined, query: string): boolean {
    const normalizedQuery = query.trim().toLowerCase();
    if (!normalizedQuery || !name) {
        return false;
    }
    return name.toLowerCase().includes(normalizedQuery);
}

export function browseSearchHitKey(hit: Omit<BrowseSearchHit, 'key'>): string {
    return [hit.kind, hit.platformUrn ?? '', hit.path.join('\u241f'), hit.label].join('|');
}

function sortBrowseSearchHits(hits: BrowseSearchHit[]): BrowseSearchHit[] {
    return [...hits].sort((left, right) => {
        const kindDelta = KIND_ORDER[left.kind] - KIND_ORDER[right.kind];
        if (kindDelta !== 0) {
            return kindDelta;
        }
        const depthDelta = left.path.length - right.path.length;
        if (depthDelta !== 0) {
            return depthDelta;
        }
        return left.label.localeCompare(right.label);
    });
}

export function extractBrowseSearchHits(entities: BrowseSearchEntity[], query: string): BrowseSearchHit[] {
    const seen = new Set<string>();
    const hits: BrowseSearchHit[] = [];

    const add = (partial: Omit<BrowseSearchHit, 'key'>) => {
        const key = browseSearchHitKey(partial);
        if (seen.has(key)) {
            return;
        }
        seen.add(key);
        hits.push({ ...partial, key });
    };

    entities.forEach((entity) => {
        const platformUrn = entity.platform?.urn;
        const platformName = entity.platform?.name;
        const parentPath = entity.browsePath.map((entry) => entry.name).filter(Boolean);
        const parentLabels = entity.browsePath.map(displayPathLabel);

        if (entity.type === EntityType.DataPlatform) {
            if (nameMatchesQuery(entity.name, query)) {
                add({
                    kind: 'platform',
                    label: entity.name,
                    platformUrn: entity.urn,
                    platformName: entity.name,
                    origin: null,
                    path: [],
                });
            }
            return;
        }

        if (platformUrn && platformName && nameMatchesQuery(platformName, query)) {
            add({
                kind: 'platform',
                label: platformName,
                platformUrn,
                platformName,
                origin: entity.origin ?? null,
                path: [],
            });
        }

        entity.browsePath.forEach((entry, index) => {
            const label = entry.displayName || entry.name;
            if (!nameMatchesQuery(label, query) && !nameMatchesQuery(entry.name, query)) {
                return;
            }
            add({
                kind: 'path',
                label,
                platformUrn,
                platformName,
                origin: entity.origin ?? null,
                path: parentPath.slice(0, index + 1),
                pathLabels: parentLabels.slice(0, index + 1),
                entity: entry.entity ?? null,
            });
        });

        if (!nameMatchesQuery(entity.name, query)) {
            return;
        }

        if (entity.type === EntityType.Container) {
            add({
                kind: 'path',
                label: entity.name,
                platformUrn,
                platformName,
                origin: entity.origin ?? null,
                path: [...parentPath, entity.urn],
                pathLabels: [...parentLabels, entity.name],
                entity: { urn: entity.urn, type: entity.type },
            });
            return;
        }

        if (parentPath.length > 0) {
            add({
                kind: 'entity',
                label: entity.name,
                platformUrn,
                platformName,
                origin: entity.origin ?? null,
                path: parentPath,
                pathLabels: parentLabels,
                entity: { urn: entity.urn, type: entity.type },
            });
        }
    });

    return sortBrowseSearchHits(hits);
}

export function applyBrowseSearchHit(hit: BrowseSearchHit, selectedFilters: FacetFilterInput[]): FacetFilterInput[] {
    const overrides: FacetFilterInput[] = [];
    const removeFields: string[] = [];

    if (hit.platformUrn) {
        overrides.push({
            field: PLATFORM_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: [hit.platformUrn],
        });
    }

    if (hit.origin) {
        overrides.push({
            field: ORIGIN_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: [hit.origin],
        });
    }

    if (hit.path.length > 0) {
        overrides.push({
            field: BROWSE_PATH_V2_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: [createBrowseV2SearchFilter(hit.path)],
        });
    } else {
        removeFields.push(BROWSE_PATH_V2_FILTER_NAME);
    }

    return applyFacetFilterOverrides(selectedFilters, overrides).filter(
        (filter) => !removeFields.includes(filter.field),
    );
}

export function withBrowsePathContainsFilter(
    orFilters: Array<{ and?: FacetFilterInput[] | null }>,
    query: string,
): Array<{ and: FacetFilterInput[] }> {
    const containFilter: FacetFilterInput = {
        field: BROWSE_PATH_V2_FILTER_NAME,
        condition: FilterOperator.Contain,
        values: [query.trim()],
    };

    if (!orFilters.length) {
        return [{ and: [containFilter] }];
    }

    return orFilters.map((group) => ({
        and: [...(group.and ?? []).filter((filter) => filter.field !== BROWSE_PATH_V2_FILTER_NAME), containFilter],
    }));
}
