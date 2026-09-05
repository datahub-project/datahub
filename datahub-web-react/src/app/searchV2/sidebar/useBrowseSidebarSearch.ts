import { useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import {
    BROWSE_SIDEBAR_SEARCH_DEBOUNCE_MS,
    BROWSE_SIDEBAR_SEARCH_PAGE_SIZE,
    type BrowseSearchEntity,
    type BrowseSearchHit,
    extractBrowseSearchHits,
    isBrowseSidebarSearchActive,
    withBrowsePathContainsFilter,
} from '@app/searchV2/sidebar/browseSidebarSearch';
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';
import { BROWSE_PATH_V2_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntitySearchResultsAutoCompleteFieldsQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

type BrowseFields = {
    browsePathV2?: {
        path?: Array<{
            name?: string | null;
            entity?: { urn: string; type: EntityType } | null;
        } | null> | null;
    } | null;
    platform?: {
        urn: string;
        name?: string | null;
        properties?: { displayName?: string | null; name?: string | null } | null;
    } | null;
    origin?: string | null;
};

function platformNameFromEntity(
    platform: NonNullable<BrowseFields['platform']>,
    registry: ReturnType<typeof useEntityRegistry>,
): string {
    return (
        registry.getDisplayName(EntityType.DataPlatform, platform) ||
        platform.properties?.displayName ||
        platform.properties?.name ||
        platform.name ||
        platform.urn
    );
}

export function toBrowseSearchEntity(
    entity: Entity,
    registry: ReturnType<typeof useEntityRegistry>,
): BrowseSearchEntity | null {
    if (!entity?.urn || !entity.type) {
        return null;
    }

    const name = registry.getDisplayName(entity.type, entity);
    if (!name) {
        return null;
    }

    if (entity.type === EntityType.DataPlatform) {
        return {
            urn: entity.urn,
            type: entity.type,
            name,
            platform: { urn: entity.urn, name },
            origin: null,
            browsePath: [],
        };
    }

    const fields = entity as Entity & BrowseFields;
    const platform = fields.platform
        ? { urn: fields.platform.urn, name: platformNameFromEntity(fields.platform, registry) }
        : null;

    const browsePath =
        fields.browsePathV2?.path
            ?.filter((entry): entry is NonNullable<typeof entry> => !!entry?.name)
            .map((entry) => {
                const displayName = entry.entity
                    ? registry.getDisplayName(entry.entity.type, entry.entity) || entry.name
                    : entry.name;
                return {
                    name: entry.name as string,
                    displayName: displayName ?? undefined,
                    entity: entry.entity ? { urn: entry.entity.urn, type: entry.entity.type } : null,
                };
            }) ?? [];

    return {
        urn: entity.urn,
        type: entity.type,
        name,
        platform,
        origin: fields.origin ?? null,
        browsePath,
    };
}

function entitiesFromSearchData(
    data:
        | { searchAcrossEntities?: { searchResults?: Array<{ entity?: Entity | null } | null> | null } | null }
        | null
        | undefined,
): Entity[] {
    return (
        data?.searchAcrossEntities?.searchResults
            ?.map((result) => result?.entity)
            .filter((entity): entity is Entity => !!entity?.urn) ?? []
    );
}

function mergeEntities(groups: Entity[][]): Entity[] {
    const seen = new Set<string>();
    const merged: Entity[] = [];
    groups.flat().forEach((entity) => {
        if (seen.has(entity.urn)) {
            return;
        }
        seen.add(entity.urn);
        merged.push(entity);
    });
    return merged;
}

type Props = {
    searchInput: string;
};

export default function useBrowseSidebarSearch({ searchInput }: Props): {
    hits: BrowseSearchHit[];
    loading: boolean;
    isRefreshing: boolean;
} {
    const registry = useEntityRegistry();
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const trimmedInput = searchInput.trim();
    const isActive = isBrowseSidebarSearchActive(searchInput);

    useDebounce(() => setDebouncedQuery(trimmedInput), BROWSE_SIDEBAR_SEARCH_DEBOUNCE_MS, [trimmedInput]);

    const { orFilters, viewUrn } = useGetSearchQueryInputs([
        BROWSE_PATH_V2_FILTER_NAME,
        PLATFORM_FILTER_NAME,
        ORIGIN_FILTER_NAME,
    ]);

    const skip = !debouncedQuery;
    const pathOrFilters = useMemo(
        () => (debouncedQuery ? withBrowsePathContainsFilter(orFilters, debouncedQuery) : []),
        [orFilters, debouncedQuery],
    );

    const textVariables = useMemo(
        () => ({
            input: {
                types: [],
                query: debouncedQuery,
                start: 0,
                count: BROWSE_SIDEBAR_SEARCH_PAGE_SIZE,
                orFilters: orFilters.length ? orFilters : undefined,
                viewUrn,
                searchFlags: { skipHighlighting: true, getSuggestions: false },
            },
            skipSiblingsSearch: true,
        }),
        [debouncedQuery, orFilters, viewUrn],
    );

    const pathVariables = useMemo(
        () => ({
            input: {
                types: [],
                query: '*',
                start: 0,
                count: BROWSE_SIDEBAR_SEARCH_PAGE_SIZE,
                orFilters: pathOrFilters,
                viewUrn,
                searchFlags: { skipHighlighting: true, getSuggestions: false },
            },
            skipSiblingsSearch: true,
        }),
        [pathOrFilters, viewUrn],
    );

    const { data: textData, loading: textLoading } = useGetEntitySearchResultsAutoCompleteFieldsQuery({
        variables: textVariables,
        skip,
        fetchPolicy: 'cache-and-network',
    });

    const { data: pathData, loading: pathLoading } = useGetEntitySearchResultsAutoCompleteFieldsQuery({
        variables: pathVariables,
        skip,
        fetchPolicy: 'cache-and-network',
    });

    const hits = useMemo(() => {
        if (!debouncedQuery) {
            return [];
        }
        const entities = mergeEntities([entitiesFromSearchData(textData), entitiesFromSearchData(pathData)])
            .map((entity) => toBrowseSearchEntity(entity, registry))
            .filter((mapped): mapped is BrowseSearchEntity => mapped !== null);
        return extractBrowseSearchHits(entities, debouncedQuery);
    }, [debouncedQuery, textData, pathData, registry]);

    const waitingForDebounce = isActive && debouncedQuery !== trimmedInput;
    const queryLoading = !skip && (textLoading || pathLoading);
    const busy = waitingForDebounce || queryLoading;

    return {
        hits: isActive ? hits : [],
        loading: busy && hits.length === 0,
        isRefreshing: busy && hits.length > 0,
    };
}
