import { useCallback, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

const PAGE_SIZE = 20;
const DEBOUNCE_MS = 300;

type UseRoleSelectorReturn = {
    roles: DataHubRole[];
    loading: boolean;
    hasMore: boolean;
    observerRef: (node: HTMLDivElement | null) => void;
    searchQuery: string;
    setSearchQuery: (query: string) => void;
    total: number;
    loadMore: () => void;
};

export function useRoleSelector(): UseRoleSelectorReturn {
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [allRoles, setAllRoles] = useState<DataHubRole[]>([]);
    const [start, setStart] = useState(0);
    const [hasMore, setHasMore] = useState(true);
    const [sentinelElement, setSentinelElement] = useState<HTMLDivElement | null>(null);

    const observerRef = useCallback((node: HTMLDivElement | null) => {
        setSentinelElement(node);
    }, []);

    useDebounce(() => setDebouncedQuery(searchQuery), DEBOUNCE_MS, [searchQuery]);

    // Reset pagination when search query changes
    useEffect(() => {
        setAllRoles([]);
        setStart(0);
        setHasMore(true);
    }, [debouncedQuery]);

    const { data, loading } = useListRolesQuery({
        fetchPolicy: 'cache-and-network',
        variables: {
            input: {
                start,
                count: PAGE_SIZE,
                query: debouncedQuery || undefined,
            },
        },
    });

    const total = data?.listRoles?.total || 0;
    const fetchedRoles = useMemo(() => {
        return (data?.listRoles?.roles || []) as DataHubRole[];
    }, [data?.listRoles?.roles]);

    // Append new roles when data arrives
    useEffect(() => {
        if (fetchedRoles.length > 0) {
            setAllRoles((prev) => {
                // If start is 0, replace all roles (new search)
                if (start === 0) {
                    return fetchedRoles;
                }
                // Otherwise append (pagination)
                const existingUrns = new Set(prev.map((r) => r.urn));
                const newRoles = fetchedRoles.filter((r) => !existingUrns.has(r.urn));
                return [...prev, ...newRoles];
            });

            // Update hasMore based on total
            setHasMore(start + fetchedRoles.length < total);
        } else if (start === 0 && !loading) {
            // No results for search
            setAllRoles([]);
            setHasMore(false);
        }
    }, [fetchedRoles, start, total, loading]);

    const loadMore = useCallback(() => {
        if (loading || !hasMore) return;
        setStart((prev) => prev + PAGE_SIZE);
    }, [loading, hasMore]);

    // IntersectionObserver for infinite scroll
    useEffect(() => {
        if (!sentinelElement || !hasMore) return undefined;

        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting) {
                    loadMore();
                }
            },
            { root: null },
        );

        observer.observe(sentinelElement);

        return () => {
            observer.disconnect();
        };
    }, [sentinelElement, loadMore, hasMore]);

    return {
        roles: allRoles,
        loading,
        hasMore,
        observerRef,
        searchQuery,
        setSearchQuery,
        total,
        loadMore,
    };
}
