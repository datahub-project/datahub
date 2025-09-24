import React, { useCallback, useMemo, useRef, useState } from 'react';

import { useBrowsePath, useEntityType } from '@app/searchV2/sidebar/BrowseContext';
import { BROWSE_LOAD_MORE_MARGIN, BROWSE_PAGE_SIZE } from '@app/searchV2/sidebar/constants';
import { useSidebarFilters } from '@app/searchV2/sidebar/useSidebarFilters';
import useIntersect from '@app/shared/useIntersect';

import { GetBrowseResultsV2Query, useGetBrowseResultsV2Query } from '@graphql/browseV2.generated';

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

    const [start, setStart] = useState(0);
    const { loading, error, refetch } = useGetBrowseResultsV2Query({
        skip,
        fetchPolicy: 'cache-first',
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
        onCompleted: (data) => {
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
        },
    });

    const retry = () => {
        if (refetch) refetch();
    };

    const advancePage = useCallback(() => {
        const newStart = latestStart + BROWSE_PAGE_SIZE;
        if (initializing.current || error || done || latestStart < 0 || total <= 0 || newStart >= total) return;
        setStart(newStart);
    }, [done, error, latestStart, total]);

    const { observableRef } = useIntersect({
        skip,
        options: { rootMargin: BROWSE_LOAD_MORE_MARGIN },
        onIntersect: advancePage,
    });

    return {
        loading: start > 0 && loading, // Don't display loading indicator for first page
        loaded: !!latestData || !!error,
        error,
        groups,
        path: latestData?.browseV2?.metadata?.path,
        observable: <div ref={observableRef} style={{ width: '1px', height: '1px' }} />,
        retry,
    } as const;
};

export default useBrowsePagination;
