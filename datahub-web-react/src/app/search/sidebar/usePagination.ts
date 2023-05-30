import { useCallback, useMemo, useRef, useState } from 'react';

type Props<Data, Item> = {
    pageSize: number;
    selectItems: (data: Data) => Array<Item>;
    selectTotal: (data: Data) => number;
};

// Generic pagination hook, could be repurposed for other queries but only used for browse sidebar for now
// It does not support virtualization but preserves loaded data in memory which is suitable for most use cases
const usePagination = <Data, Item>({ pageSize, selectItems, selectTotal }: Props<Data, Item>) => {
    const map = useRef(new Map<number, Data>());
    const [startList, setStartList] = useState<Array<number>>([]);
    const [currentPage, setCurrentPage] = useState(0);
    const items = useMemo(
        () =>
            startList.flatMap((s) => {
                const data = map.current.get(s);
                return data ? selectItems(data) : [];
            }),
        [selectItems, startList],
    );
    const latestStart = startList.length ? startList[startList.length - 1] : -1;
    const latestData = latestStart >= 0 ? map.current.get(latestStart) : null;
    const total = latestData ? selectTotal(latestData) : -1;
    const done = !!latestData && items.length >= total;

    const appendPage = useCallback((newStart: number, data: Data) => {
        map.current.set(newStart, data);
        setStartList((sl) => [...sl, newStart]);
    }, []);

    const advancePage = useCallback(() => {
        const newStart = latestStart + pageSize;
        if (done || latestStart < 0 || total <= 0 || newStart >= total) return;
        setCurrentPage(newStart);
    }, [done, latestStart, pageSize, total]);

    const hasPage = useCallback((page: number) => map.current.has(page), []);

    return {
        currentPage,
        items,
        latestData,
        total,
        done,
        appendPage,
        advancePage,
        hasPage,
    } as const;
};

export default usePagination;
