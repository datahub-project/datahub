import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import useIntersect from '../../shared/useIntersect';
import { BROWSE_LOAD_MORE_MARGIN, BROWSE_PAGE_SIZE } from './constants';
import { GetBrowseResultsV2Query, useGetBrowseResultsV2LazyQuery } from '../../../graphql/browseV2.generated';
import { useSidebarFilters } from './useSidebarFilters';
import { useBrowsePath, useEntityType } from './BrowseContext';

type Props = {
    skip: boolean;
};

const useBrowsePagination = ({ skip }: Props) => {
    const initializing = useRef(true);
    const type = useEntityType();
    const path = useBrowsePath();
    const sidebarFilters = useSidebarFilters();
    const [startToBrowseMap, setStartToBrowseMap] = useState<Record<number, GetBrowseResultsV2Query | undefined>>({});
    const startList = useMemo(
        () =>
            Object.keys(startToBrowseMap)
                .map(Number)
                .sort((a, b) => a - b),
        [startToBrowseMap],
    );
    const groups = useMemo(
        () => startList.flatMap((start) => startToBrowseMap[start]?.browseV2?.groups ?? []),
        [startList, startToBrowseMap],
    );
    const latestStart = startList.length ? startList[startList.length - 1] : -1;
    const latestData = latestStart >= 0 ? startToBrowseMap[latestStart] : null;
    const total = latestData?.browseV2?.total ?? -1;
    const done = !!latestData && groups.length >= total;

    const [getBrowseResultsV2, { data, error, refetch }] = useGetBrowseResultsV2LazyQuery({
        fetchPolicy: 'cache-first',
    });

    const retry = () => {
        if (refetch) refetch();
    };

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
                        orFilters: sidebarFilters.orFilters,
                        viewUrn: sidebarFilters.viewUrn,
                        query: sidebarFilters.query,
                    },
                },
            });
        },
        [getBrowseResultsV2, path, sidebarFilters.orFilters, sidebarFilters.query, sidebarFilters.viewUrn, skip, type],
    );

    useEffect(() => {
        initializing.current = true;
        getBrowseResultsV2WithDeps(0);
    }, [getBrowseResultsV2WithDeps]);

    useEffect(() => {
        const newStart = data?.browseV2?.start ?? -1;
        if (newStart === 0) initializing.current = false;
        if (initializing.current || !data || newStart < 0) return;

        setStartToBrowseMap((previousMap) => {
            const newMap: typeof previousMap = { [newStart]: data };

            Object.keys(previousMap)
                .map(Number)
                .forEach((previousStart) => {
                    if (previousStart < newStart) newMap[previousStart] = previousMap[previousStart];
                });

            return newMap;
        });
    }, [data]);

    const advancePage = useCallback(() => {
        const newStart = latestStart + BROWSE_PAGE_SIZE;
        if (initializing.current || error || done || latestStart < 0 || total <= 0 || newStart >= total) return;
        getBrowseResultsV2WithDeps(newStart);
    }, [done, error, getBrowseResultsV2WithDeps, latestStart, total]);

    const { observableRef } = useIntersect({
        skip,
        options: { rootMargin: BROWSE_LOAD_MORE_MARGIN },
        onIntersect: advancePage,
    });

    return {
        loaded: !!latestData || !!error,
        error,
        groups,
        path: latestData?.browseV2?.metadata?.path,
        observable: <div ref={observableRef} style={{ width: '1px', height: '1px' }} />,
        retry,
    } as const;
};

export default useBrowsePagination;
