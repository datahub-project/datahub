import React, { useCallback, useEffect, useMemo, useState } from 'react';
import useIntersect from '../../shared/useIntersect';
import { BROWSE_LOAD_MORE_MARGIN, BROWSE_PAGE_SIZE } from './constants';
import { GetBrowseResultsV2Query, useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';
import { useSidebarFilters } from './useSidebarFilters';
import { useBrowsePath, useEntityType } from './BrowseContext';
import { SidebarFilters } from './types';

type Props = {
    skip: boolean;
};

type State = {
    current: number;
    list: Array<number>;
    map: Record<number, GetBrowseResultsV2Query>;
    filters: SidebarFilters | null;
};

const getInitialState = (): State => ({
    current: 0,
    list: [],
    filters: null,
    map: {},
});

const useBrowsePagination = ({ skip }: Props) => {
    const type = useEntityType();
    const path = useBrowsePath();
    const sidebarFilters = useSidebarFilters();
    const [{ current, list, map, filters }, setState] = useState(getInitialState);
    const groups = useMemo(() => list.flatMap((start) => map[start]?.browseV2?.groups ?? []), [list, map]);
    const latestStart = list.length ? list[list.length - 1] : -1;
    const latestData = latestStart >= 0 ? map[latestStart] : null;
    const total = latestData?.browseV2?.total ?? -1;
    const done = !!latestData && groups.length >= total;

    const { data, error, refetch } = useGetBrowseResultsV2Query({
        skip: skip || !filters,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                type,
                path,
                start: current,
                count: BROWSE_PAGE_SIZE,
                orFilters: filters?.orFilters,
                viewUrn: filters?.viewUrn,
                query: filters?.query,
            },
        },
    });

    // What's happening here is that we have no entity filter before, but then we add one
    // what we could do is move the entityFilters into the list
    useEffect(() => {
        setState(() => ({
            current: 0,
            list: [],
            map: {},
            filters: sidebarFilters,
        }));
    }, [sidebarFilters]);

    useEffect(() => {
        const newStart = data?.browseV2?.start ?? -1;
        if (!data || newStart < 0) return;
        setState((s) =>
            s.map[newStart]
                ? s
                : {
                      ...s,
                      list: [...s.list, newStart].sort((a, b) => a - b),
                      map: { ...s.map, [newStart]: data },
                  },
        );
    }, [data]);

    const advancePage = useCallback(() => {
        // todo - remove
        if (1) return;
        const newStart = latestStart + BROWSE_PAGE_SIZE;
        if (done || latestStart < 0 || total <= 0 || newStart >= total) return;
        setState((s) => ({ ...s, current: newStart }));
    }, [done, latestStart, total]);

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
