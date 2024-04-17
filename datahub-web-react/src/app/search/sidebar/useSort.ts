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
