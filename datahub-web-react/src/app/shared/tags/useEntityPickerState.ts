import React, { useCallback, useEffect, useMemo, useState } from 'react';

import { useGetRecommendations } from '@app/shared/recommendation';

import { useGetAutoCompleteResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

interface UseEntityPickerStateArgs {
    entityType: EntityType.Tag | EntityType.GlossaryTerm;
    /** Pre-populates `urns` + the entity cache so the chip strip has labels on first render. */
    defaultValues?: { urn: string; entity?: Entity | null }[];
    /** Max autocomplete results requested per search keystroke. Matches the legacy modal default. */
    limit?: number;
}

export interface UseEntityPickerStateResult {
    urns: string[];
    setUrns: React.Dispatch<React.SetStateAction<string[]>>;
    removeUrn: (urn: string) => void;
    /** URN → entity map of everything we've seen, so the chip strip can still render after a search clears. */
    entityCache: Record<string, Entity>;
    searchText: string;
    handleSearch: (text: string) => void;
    /** Either the latest autocomplete results (when searching) or the recommendations (initial state). */
    currentEntities: Entity[];
    isLoading: boolean;
}

/**
 * Shared state + autocomplete plumbing for the tag/term picker modals.
 *
 * Both `AddTagsModal` and `AddTermsModal` need the same picker behavior — selected URNs, a cache
 * of every entity we've seen so the chip strip can render after a search clears, a search box
 * wired to the autocomplete API, and a recommendations fallback for the initial dropdown. The
 * only difference is the entity type, so it's parameterised here and the modals just consume the
 * result.
 */
export function useEntityPickerState({
    entityType,
    defaultValues = [],
    limit = 10,
}: UseEntityPickerStateArgs): UseEntityPickerStateResult {
    const [urns, setUrns] = useState<string[]>(defaultValues.map((v) => v.urn));
    const [entityCache, setEntityCache] = useState<Record<string, Entity>>(() => {
        const cache: Record<string, Entity> = {};
        defaultValues.forEach(({ urn, entity }) => {
            if (entity) cache[urn] = entity;
        });
        return cache;
    });
    const [searchText, setSearchText] = useState('');

    const [autoComplete, { data: searchData, loading: searchLoading }] = useGetAutoCompleteResultsLazyQuery();
    const { recommendedData, loading: recommendationsLoading } = useGetRecommendations([entityType]);

    const handleSearch = useCallback(
        (text: string) => {
            const trimmed = text.trim();
            setSearchText(trimmed);
            if (trimmed.length > 0) {
                autoComplete({ variables: { input: { type: entityType, query: trimmed, limit } } });
            }
        },
        [autoComplete, entityType, limit],
    );

    const searchEntities = useMemo<Entity[]>(
        () => (searchData?.autoComplete?.entities as Entity[] | undefined) || [],
        [searchData],
    );
    const initialEntities = useMemo<Entity[]>(
        () => (!searchText ? (recommendedData as Entity[] | undefined) || [] : []),
        [searchText, recommendedData],
    );
    const currentEntities = searchText ? searchEntities : initialEntities;

    useEffect(() => {
        if (currentEntities.length === 0) return;
        setEntityCache((prev) => {
            const next = { ...prev };
            let changed = false;
            currentEntities.forEach((e) => {
                if (!next[e.urn]) {
                    next[e.urn] = e;
                    changed = true;
                }
            });
            return changed ? next : prev;
        });
    }, [currentEntities]);

    const removeUrn = useCallback((urn: string) => setUrns((prev) => prev.filter((u) => u !== urn)), []);

    return {
        urns,
        setUrns,
        removeUrn,
        entityCache,
        searchText,
        handleSearch,
        currentEntities,
        isLoading: searchLoading || recommendationsLoading,
    };
}
