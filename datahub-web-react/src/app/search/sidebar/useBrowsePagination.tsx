import React, { useCallback, useEffect, useMemo, useState } from 'react';
import useIntersect from '../../shared/useIntersect';
import { BROWSE_LOAD_MORE_MARGIN, BROWSE_PAGE_SIZE } from './constants';
import { GetBrowseResultsV2Query, useGetBrowseResultsV2LazyQuery } from '../../../graphql/browseV2.generated';
import { useSidebarFilters } from './useSidebarFilters';
import { useBrowsePath, useEntityType } from './BrowseContext';

type Props = {
    skip: boolean;
};

type State = {
    list: Array<number>;
    map: Record<number, GetBrowseResultsV2Query>;
};

const getInitialState = (): State => ({
    list: [],
    map: {},
});

const useBrowsePagination = ({ skip }: Props) => {
    const type = useEntityType();
    const path = useBrowsePath();
    const sidebarFilters = useSidebarFilters();
    const [{ list, map }, setState] = useState(getInitialState);
    const groups = useMemo(() => list.flatMap((start) => map[start]?.browseV2?.groups ?? []), [list, map]);
    const latestStart = list.length ? list[list.length - 1] : -1;
    const latestData = latestStart >= 0 ? map[latestStart] : null;
    const total = latestData?.browseV2?.total ?? -1;
    const done = !!latestData && groups.length >= total;

    const [getBrowseResultsV2, { data, error, refetch }] = useGetBrowseResultsV2LazyQuery({
        fetchPolicy: 'cache-first',
    });

    const getBrowseResultsV2WithDeps = useCallback(
        (start: number) => {
            if (skip) return;
            getBrowseResultsV2({
                variables: {
                    input: {
                        type,
                        path,
                        start,
                        count: BROWSE_PAGE_SIZE,
                        orFilters: sidebarFilters?.orFilters,
                        viewUrn: sidebarFilters?.viewUrn,
                        query: sidebarFilters?.query,
                    },
                },
            });
        },
        [
            getBrowseResultsV2,
            path,
            sidebarFilters?.orFilters,
            sidebarFilters?.query,
            sidebarFilters?.viewUrn,
            skip,
            type,
        ],
    );

    useEffect(() => {
        getBrowseResultsV2WithDeps(0);
    }, [getBrowseResultsV2WithDeps]);

    useEffect(() => {
        const newStart = data?.browseV2?.start ?? -1;
        if (!data || newStart < 0) return;
        setState((state) => {
            const newList: typeof state.list = state.list.filter((start) => start < newStart);
            const newMap: typeof state.map = {};

            for (let i = 0; i < newList.length; i++) newMap[newList[i]] = state.map[newList[i]];

            newList.push(newStart);
            newMap[newStart] = data;

            return {
                list: newList,
                map: newMap,
            };
        });
    }, [data]);

    const advancePage = useCallback(() => {
        const newStart = latestStart + BROWSE_PAGE_SIZE;
        if (done || latestStart < 0 || total <= 0 || newStart >= total) return;
        getBrowseResultsV2WithDeps(newStart);
    }, [done, getBrowseResultsV2WithDeps, latestStart, total]);

    const { observableRef } = useIntersect({
        skip,
        options: { rootMargin: BROWSE_LOAD_MORE_MARGIN },
        onIntersect: advancePage,
    });

    return {
        loaded: !!latestData || !!error,
        error,
        groups,
        path: latestData?.browseV2?.metadata.path,
        observable: <div ref={observableRef} style={{ width: '1px', height: '1px' }} />,
        refetch,
    } as const;
};

export default useBrowsePagination;
