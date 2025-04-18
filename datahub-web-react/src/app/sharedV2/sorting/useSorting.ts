import { useState } from 'react';
import { SortOrder } from '../../../types.generated';

export interface Sorting {
    sortField: string | null;
    setSortField: React.Dispatch<React.SetStateAction<string | null>>;
    sortOrder: SortOrder;
    setSortOrder: (sortOrder: string | SortOrder | null) => void;
}

export default function useSorting() {
    const [sortField, setSortField] = useState<string | null>(null);
    const [sortOrder, setSortOrder] = useState<SortOrder | null>(SortOrder.Ascending);

    function handleSetSortOrder(newOrder: string | SortOrder | null) {
        if (newOrder?.toLocaleLowerCase() === SortOrder.Ascending.toLocaleLowerCase() || newOrder === 'ascend') {
            setSortOrder(SortOrder.Ascending);
        } else if (
            newOrder?.toLocaleLowerCase() === SortOrder.Descending.toLocaleLowerCase() ||
            newOrder === 'descend'
        ) {
            setSortOrder(SortOrder.Descending);
        } else {
            setSortOrder(null);
        }
    }

    return { sortField, setSortField, sortOrder, setSortOrder: handleSetSortOrder } as Sorting;
}
