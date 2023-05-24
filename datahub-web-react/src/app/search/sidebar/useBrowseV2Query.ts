import { useCallback, useEffect, useRef, useState } from 'react';
import { EntityType } from '../../../types.generated';
import { useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';
import useSidebarFilters from './useSidebarFilters';
import { BROWSE_PATH_PAGE_SIZE } from './constants';
import useBrowseMap from './useBrowseMap';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform?: string | null;
    path: Array<string>;
    skip: boolean;
};

const useBrowseV2Query = ({ entityType, environment, platform, path, skip }: Props) => {
    const locked = useRef(skip);
    const { query, orFilters, viewUrn } = useSidebarFilters({ environment, platform });
    const { hasData, latestStart, groups, pathResult, done, total, mapAppend, mapClear } = useBrowseMap();
    const [currentStart, setCurrentStart] = useState(0);
    const [cachedFilters, setCachedFilters] = useState<ReturnType<typeof useSidebarFilters>>();

    const { data, loading, error } = useGetBrowseResultsV2Query({
        skip,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                type: entityType,
                path,
                start: currentStart,
                count: BROWSE_PATH_PAGE_SIZE,
                ...cachedFilters,
            },
        },
    });

    useEffect(() => {
        mapClear();
        setCurrentStart(0);
        setCachedFilters({ query, orFilters, viewUrn });
    }, [mapClear, orFilters, query, viewUrn]);

    const loaded = hasData || !!error;

    useEffect(() => {
        mapAppend(data);
    }, [mapAppend, data]);

    useEffect(() => {
        if (!skip && latestStart >= currentStart) locked.current = false;
    }, [currentStart, latestStart, skip]);

    const loadMore = useCallback(
        () =>
            setCurrentStart((current) => {
                const newStart = current + BROWSE_PATH_PAGE_SIZE;
                if (locked.current || done || total <= 0 || newStart >= total) return current;
                locked.current = true;
                return newStart;
            }),
        [done, total],
    );

    return {
        loading,
        loaded,
        error,
        groups,
        pathResult,
        loadMore,
    } as const;
};

export default useBrowseV2Query;
