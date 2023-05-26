import { useCallback, useEffect, useState } from 'react';
import { EntityType } from '../../../types.generated';
import { useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';
import useSidebarFilters from './useSidebarFilters';
import { BROWSE_PATH_PAGE_SIZE } from './constants';
import usePaginatedBrowse from './usePaginatedBrowse';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform?: string | null;
    path: Array<string>;
    skip: boolean;
};

const useBrowseV2Query = ({ entityType, environment, platform, path, skip }: Props) => {
    const [currentStart, setCurrentStart] = useState(0);
    const sidebarFilters = useSidebarFilters({ environment, platform });
    const [cachedFilters, setCachedFilters] = useState(sidebarFilters);
    const { hasPages, groups, pathResult, done, total, appendPage, clearPages } = usePaginatedBrowse();

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

    const loaded = hasPages || !!error;

    useEffect(() => {
        clearPages();
        setCurrentStart(0);
        setCachedFilters(sidebarFilters);
    }, [clearPages, sidebarFilters]);

    const fetchNextPage = useCallback(
        () =>
            setCurrentStart((current) => {
                const newStart = current + BROWSE_PATH_PAGE_SIZE;
                if (done || total <= 0 || newStart >= total) {
                    console.log('current', current);
                    return current;
                }
                console.log('newStart', newStart);
                return newStart;
            }),
        [done, total],
    );

    useEffect(() => {
        appendPage(data);
    }, [appendPage, data]);

    return {
        loading,
        loaded,
        error,
        groups,
        pathResult,
        fetchNextPage,
    } as const;
};

export default useBrowseV2Query;
