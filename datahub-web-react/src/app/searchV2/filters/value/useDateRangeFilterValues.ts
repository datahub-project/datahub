import { useEffect, useMemo } from 'react';
import { FilterOperator } from '@src/types.generated';
import moment from 'moment';
import { Datetime } from '@src/app/lineageV2/LineageTimeSelector';
import useGetSearchQueryInputs from '../../useGetSearchQueryInputs';

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
            setStartDate(
                moment(parseInt(startDateFromFilter, 10)).set({ hour: 0, minute: 0, second: 0, millisecond: 0 }),
            );
        } else {
            setStartDate(null);
        }
    }, [startDateFromFilter, setStartDate]);

    useEffect(() => {
        if (endDateFromFilter) {
            setEndDate(
                moment(parseInt(endDateFromFilter, 10)).set({ hour: 23, minute: 59, second: 59, millisecond: 999 }),
            );
        } else {
            setEndDate(null);
        }
    }, [endDateFromFilter, setEndDate]);
}
