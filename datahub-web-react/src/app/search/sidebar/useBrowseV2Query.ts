import { useEffect, useMemo } from 'react';
import { BrowseResultGroupV2 } from '../../../types.generated';
import { BROWSE_PAGE_SIZE } from './constants';
import usePagination from './usePagination';
import { GetBrowseResultsV2Query, useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';
import { useBrowsePath, useEntityType, useFilters } from './BrowseContext';

type Props = {
    skip: boolean;
};

const useBrowseV2Query = ({ skip }: Props) => {
    const type = useEntityType();
    const path = useBrowsePath();
    const filters = useFilters();

    const {
        currentPage,
        items: groups,
        latestData,
        appendPage,
        advancePage,
        hasPage,
    } = usePagination<GetBrowseResultsV2Query, BrowseResultGroupV2>(
        useMemo(
            () => ({
                pageSize: BROWSE_PAGE_SIZE,
                selectItems: (data) => data.browseV2?.groups ?? [],
                selectTotal: (data) => data.browseV2?.total ?? -1,
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
                type,
                path,
                start: currentPage,
                count: BROWSE_PAGE_SIZE,
                ...filters,
            },
        },
    });

    const loaded = !!latestData || !!error;

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
