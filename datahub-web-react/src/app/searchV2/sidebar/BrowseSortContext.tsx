import React, { ReactNode, createContext, useContext, useMemo, useState } from 'react';

export enum BrowseSortOrder {
    ALPHABETICAL_ASC = 'ALPHABETICAL_ASC',
    ALPHABETICAL_DESC = 'ALPHABETICAL_DESC',
    RECENTLY_USED = 'RECENTLY_USED',
}

type BrowseSortContextValue = {
    sortOrder: BrowseSortOrder;
    setSortOrder: React.Dispatch<React.SetStateAction<BrowseSortOrder>>;
};

const BrowseSortContext = createContext<BrowseSortContextValue | null>(null);

type Props = {
    children: ReactNode;
    initialSortOrder?: BrowseSortOrder;
};

export const BrowseSortProvider = ({
    children,
    initialSortOrder = BrowseSortOrder.ALPHABETICAL_ASC,
}: Props) => {
    const [sortOrder, setSortOrder] = useState(initialSortOrder);
    const value = useMemo(() => ({ sortOrder, setSortOrder }), [sortOrder]);

    return <BrowseSortContext.Provider value={value}>{children}</BrowseSortContext.Provider>;
};

export const useBrowseSortOrder = () => {
    const context = useContext(BrowseSortContext);

    if (!context) {
        throw new Error(`${useBrowseSortOrder.name} must be used under a ${BrowseSortProvider.name}`);
    }

    return context.sortOrder;
};

export const useSetBrowseSortOrder = () => {
    const context = useContext(BrowseSortContext);

    if (!context) {
        throw new Error(`${useSetBrowseSortOrder.name} must be used under a ${BrowseSortProvider.name}`);
    }

    return context.setSortOrder;
};
