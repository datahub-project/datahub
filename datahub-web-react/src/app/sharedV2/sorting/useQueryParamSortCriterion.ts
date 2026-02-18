import { useCallback, useMemo, useState } from 'react';

import { useUrlQueryParam } from '@app/shared/useUrlQueryParam';

import { SortCriterion, SortOrder } from '@types';

export function useQueryParamSortCriterion(defaultValue?: SortCriterion) {
    const { value: fieldValue, setValue: setFieldValue } = useUrlQueryParam('sort_field', defaultValue?.field);
    const { value: orderValue, setValue: setOrderValue } = useUrlQueryParam('order', defaultValue?.sortOrder);

    const urlParamSortCriterion: SortCriterion | undefined = useMemo(() => {
        if (!fieldValue) return undefined;
        if (!orderValue) return undefined;

        const sortOrder = orderValue.toUpperCase();
        if (sortOrder === SortOrder.Ascending || sortOrder === SortOrder.Descending) {
            return {
                field: fieldValue,
                sortOrder: sortOrder as SortOrder,
            };
        }

        return undefined;
    }, [fieldValue, orderValue]);

    const [sort, setSort] = useState<SortCriterion | undefined>(urlParamSortCriterion);

    const updateSort = useCallback(
        (newSort: SortCriterion | undefined) => {
            if (newSort === undefined) {
                setFieldValue('');
                setOrderValue('');
            } else {
                setFieldValue(newSort.field);
                setOrderValue(newSort.sortOrder);
            }

            setSort(newSort);
        },
        [setFieldValue, setOrderValue],
    );

    return { sort, setSort: updateSort };
}
