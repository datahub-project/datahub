import { useEffect, useMemo } from 'react';

import useGetSearchQueryInputs from '@app/search/useGetSearchQueryInputs';
import { FilterOperator } from '@src/types.generated';
import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';

export type Datetime = Dayjs | null;

interface Props {
    filterField: string;
    setStartDate: React.Dispatch<React.SetStateAction<Datetime>>;
    setEndDate: React.Dispatch<React.SetStateAction<Datetime>>;
}

export default function useDateRangeFilterValues({ filterField, setStartDate, setEndDate }: Props) {
    const { filters: activeFilters } = useGetSearchQueryInputs();
    const matchingFilters = useMemo(
        () => activeFilters.filter((f) => f.field === filterField),
        [activeFilters, filterField],
    );
    const startDateFromFilter = useMemo(
        () => matchingFilters.find((f) => f.condition === FilterOperator.GreaterThan)?.values?.[0],
        [matchingFilters],
    );
    const endDateFromFilter = useMemo(
        () => matchingFilters.find((f) => f.condition === FilterOperator.LessThan)?.values?.[0],
        [matchingFilters],
    );

    useEffect(() => {
        if (startDateFromFilter) {
            setStartDate(dayjs(parseInt(startDateFromFilter, 10)).startOf('day'));
        } else {
            setStartDate(null);
        }
    }, [startDateFromFilter, setStartDate]);

    useEffect(() => {
        if (endDateFromFilter) {
            setEndDate(dayjs(parseInt(endDateFromFilter, 10)).endOf('day'));
        } else {
            setEndDate(null);
        }
    }, [endDateFromFilter, setEndDate]);
}
