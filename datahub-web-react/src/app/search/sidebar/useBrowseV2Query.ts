import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { EntityType } from '../../../types.generated';
import { GetBrowseResultsV2Query, useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';
import useSidebarFilters from './useSidebarFilters';
import { BROWSE_PATH_PAGE_SIZE } from './constants';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform?: string | null;
    path: Array<string>;
    skip: boolean;
};

const useBrowseV2Query = ({ entityType, environment, platform, path, skip }: Props) => {
    const [startList, setStartList] = useState<Array<number>>([]);
    const map = useRef(new Map<number, GetBrowseResultsV2Query>());
    const [currentStart, setCurrentStart] = useState(0);
    const sidebarFilters = useSidebarFilters({ environment, platform });
    const [cachedFilters, setCachedFilters] = useState(sidebarFilters);

    const groups = useMemo(() => startList.flatMap((s) => map.current.get(s)?.browseV2?.groups ?? []), [startList]);
    const latestStart = startList.length ? startList[startList.length - 1] : -1;
    const latestData = latestStart >= 0 ? map.current.get(latestStart) : null;
    const pathResult = latestData?.browseV2?.metadata.path ?? [];
    const total = latestData?.browseV2?.total ?? -1;
    const done = !!latestData && groups.length >= total;
    const hasPages = !!latestData;

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

    const fetchNextPage = useCallback(
        () =>
            setCurrentStart((current) => {
                const newStart = current + BROWSE_PATH_PAGE_SIZE;
                if (done || total <= 0 || newStart >= total) return current;
                return newStart;
            }),
        [done, total],
    );

    useEffect(() => {
        map.current.clear();
        setStartList(() => []);
        setCurrentStart(0);
        setCachedFilters(sidebarFilters);
    }, [sidebarFilters]);

    useEffect(() => {
        const newStart = data?.browseV2?.start ?? -1;
        if (!data || newStart < 0 || map.current.has(newStart)) return;
        map.current.set(newStart, data);
        setStartList((current) => [...current, newStart].sort());
    }, [data]);

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
