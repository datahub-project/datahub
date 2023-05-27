import { useCallback, useMemo, useState } from 'react';

type PageState<Data> = {
    map: Map<number, Data>;
    list: Array<number>;
    current: number;
};

type Props<Data, Item> = {
    pageSize: number;
    getItems: (data: Data) => Array<Item>;
    getTotal: (data: Data) => number;
};

const createDefaultPageState = <Data>(): PageState<Data> => ({
    list: [],
    current: 0,
    map: new Map<number, Data>(),
});

// Generic pagination hook, could be repurposed for other queries but only used for browse sidebar for now
// It does not support virtualization but preserves loaded data in memory which is suitable for most use cases
const usePagination = <Data, Item>({ pageSize, getItems, getTotal }: Props<Data, Item>) => {
    const [pageState, setPageState] = useState(() => createDefaultPageState<Data>());
    const items = useMemo(
        () =>
            pageState.list.flatMap((s) => {
                const data = pageState.map.get(s);
                return data ? getItems(data) : [];
            }),
        [getItems, pageState.list, pageState.map],
    );
    const latestStart = pageState.list.length ? pageState.list[pageState.list.length - 1] : -1;
    const latestData = latestStart >= 0 ? pageState.map.get(latestStart) : null;
    const total = latestData ? getTotal(latestData) : -1;
    const done = !!latestData && items.length >= total;

    const resetPages = useCallback(() => setPageState(createDefaultPageState<Data>()), []);

    const appendPage = useCallback(
        (newStart: number, data: Data) =>
            setPageState((state) => {
                pageState.map.set(newStart, data);
                return {
                    ...state,
                    list: [...state.list, newStart],
                };
            }),
        [pageState.map],
    );

    const advancePage = useCallback(() => {
        setPageState((state) => {
            const newStart = latestStart + pageSize;
            if (done || latestStart < 0 || total <= 0 || newStart >= total) return state;
            return {
                ...state,
                current: newStart,
            };
        });
    }, [done, latestStart, pageSize, total]);

    const hasPage = useCallback((page: number) => pageState.map.has(page), [pageState.map]);

    const currentPage = pageState.current;

    return {
        currentPage,
        items,
        latestData,
        total,
        done,
        setPageState,
        resetPages,
        appendPage,
        advancePage,
        hasPage,
    } as const;
};

export default usePagination;
