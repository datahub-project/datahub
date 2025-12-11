/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useState } from 'react';

import { SortOrder } from '@types';

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
