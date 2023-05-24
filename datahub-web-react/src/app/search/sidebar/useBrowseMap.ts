import { useCallback, useMemo, useRef, useState } from 'react';
import { GetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';

const useBrowseMap = () => {
    const [startList, setStartList] = useState<Array<number>>([]);
    const map = useRef(new Map<number, GetBrowseResultsV2Query>());

    const groups = useMemo(() => startList.flatMap((s) => map.current.get(s)?.browseV2?.groups ?? []), [startList]);
    const latestStart = startList.length ? startList[startList.length - 1] : -1;
    const latestData = latestStart >= 0 ? map.current.get(latestStart) : null;
    const pathResult = latestData?.browseV2?.metadata.path ?? [];
    const total = latestData?.browseV2?.total ?? -1;
    const done = !!latestData && groups.length >= total;
    const hasData = !!latestData;

    const mapAppend = useCallback((data?: GetBrowseResultsV2Query) => {
        const newStart = data?.browseV2?.start ?? -1;
        if (!data || newStart < 0 || map.current.has(newStart)) return;
        map.current.set(newStart, data);
        setStartList((current) => [...current, newStart].sort());
    }, []);

    const mapClear = useCallback(() => {
        setStartList(() => {
            map.current.clear();
            return [];
        });
    }, []);

    return { hasData, latestStart, groups, pathResult, done, total, mapAppend, mapClear } as const;
};

export default useBrowseMap;
