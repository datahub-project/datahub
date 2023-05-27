import { useEffect, useMemo, useState } from 'react';
import { BrowseResultGroupV2, EntityType } from '../../../types.generated';
import useSidebarFilters from './useSidebarFilters';
import { BROWSE_PAGE_SIZE } from './constants';
import usePagination from './usePagination';
import { GetBrowseResultsV2Query, useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform?: string | null;
    path: Array<string>;
    skip: boolean;
};

const useBrowseV2Query = ({ entityType, environment, platform, path, skip }: Props) => {
    const sidebarFilters = useSidebarFilters({ environment, platform });
    const [cachedFilters, setCachedFilters] = useState(sidebarFilters);

    const {
        currentPage,
        items: groups,
        latestData,
        resetPages,
        appendPage,
        advancePage,
        hasPage,
    } = usePagination<GetBrowseResultsV2Query, BrowseResultGroupV2>(
        useMemo(
            () => ({
                pageSize: BROWSE_PAGE_SIZE,
                getItems: (data) => data.browseV2?.groups ?? [],
                getTotal: (data) => data.browseV2?.total ?? -1,
            }),
            [],
        ),
    );

    const pathResult = latestData?.browseV2?.metadata.path ?? [];

    const { data, loading, error, refetch } = useGetBrowseResultsV2Query({
        skip,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                type: entityType,
                path,
                start: currentPage,
                count: BROWSE_PAGE_SIZE,
                ...cachedFilters,
            },
        },
    });

    const loaded = !!latestData || !!error;

    useEffect(() => {
        resetPages();
        setCachedFilters(sidebarFilters);
    }, [resetPages, sidebarFilters]);

    useEffect(() => {
        const newStart = data?.browseV2?.start ?? -1;
        if (!data || newStart < 0 || hasPage(newStart)) return;
        appendPage(newStart, data);
    }, [appendPage, data, hasPage]);

    return {
        loading,
        loaded,
        error,
        groups,
        pathResult,
        advancePage,
        refetch,
    } as const;
};

export default useBrowseV2Query;
