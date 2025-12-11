/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

export type SortBy = 'AtoZ' | 'ZtoA';

export const useSort = (initialData, sortBy: SortBy) => {
    const [sortedData, setSortedData] = useState([...initialData]);

    useEffect(() => {
        const sortData = () => {
            const newData = [...initialData];
            if (sortBy === 'AtoZ') {
                newData?.sort((a, b) => {
                    const nameA = a?.entity?.properties?.name || a.name;
                    const nameB = b?.entity?.properties?.name || b.name;
                    return nameA?.localeCompare(nameB);
                });
            } else if (sortBy === 'ZtoA') {
                newData?.sort((a, b) => {
                    const nameA = a?.entity?.properties?.name || a.name;
                    const nameB = b?.entity?.properties?.name || b.name;
                    return nameB?.localeCompare(nameA);
                });
            }
            setSortedData(newData);
        };
        sortData();
    }, [initialData, sortBy]);

    return sortedData;
};
