import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import useIntersect from '../../shared/useIntersect';
import { BROWSE_LOAD_MORE_MARGIN, BROWSE_PAGE_SIZE } from './constants';
import { GetBrowseResultsV2Query, useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';
import useSidebarFilters, { SidebarFilters } from './useSidebarFilters';
import { useBrowsePath, useEntityType } from './BrowseContext';

type Props = {
    skip: boolean;
};

type State = {
    current: number;
    list: Array<number>;
    filters: SidebarFilters | null;
};

const getInitialState = (): State => ({
    current: 0,
    list: [],
    filters: null,
});

const useBrowsePagination = ({ skip }: Props) => {
    const type = useEntityType();
    const path = useBrowsePath();
    const latestSidebarFilters = useSidebarFilters();
    const map = useRef(new Map<number, GetBrowseResultsV2Query>());
    const [state, setState] = useState(getInitialState);
    const groups = useMemo(() => state.list.flatMap((s) => map.current.get(s)?.browseV2?.groups ?? []), [state.list]);
    const latestStart = state.list.length ? state.list[state.list.length - 1] : -1;
    const latestData = latestStart >= 0 ? map.current.get(latestStart) : null;
    const total = latestData?.browseV2?.total ?? -1;
    const done = !!latestData && groups.length >= total;

    useEffect(() => {
        // todo - maybe there's another way to synchronize the map, by putting it in state?
        // but then I'll have some hooks now depending on it which may be an issue?

        // what if a query was already in flight?
        // we literally want a fresh map, probably?
        map.current.clear();
        setState({
            current: 0,
            list: [],
            filters: latestSidebarFilters,
        });
    }, [latestSidebarFilters]);

    const { data, error, refetch } = useGetBrowseResultsV2Query({
        skip: skip || !state.filters,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                type,
                path,
                start: state.current,
                count: BROWSE_PAGE_SIZE,
                ...state.filters,
            },
        },
    });

    const advancePage = useCallback(() => {
        const newStart = latestStart + BROWSE_PAGE_SIZE;
        if (done || latestStart < 0 || total <= 0 || newStart >= total) return;
        setState((s) => ({ ...s, current: newStart }));
    }, [done, latestStart, total]);

    useEffect(() => {
        const newStart = data?.browseV2?.start ?? -1;
        if (!data || newStart < 0 || map.current.has(newStart)) return;
        map.current.set(newStart, data);
        setState((s) => ({ ...s, list: [...s.list, newStart] }));
    }, [data]);

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
