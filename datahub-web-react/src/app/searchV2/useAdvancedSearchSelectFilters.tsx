import { useState } from 'react';
import { FacetFilterInput, FilterOperator } from '../../types.generated';
import { TEXT_FIELDS } from './utils/constants';

interface Props {
    selectedFilters: FacetFilterInput[];
    onFilterSelect: (newFilters: FacetFilterInput[]) => void;
}

export default function useAdvancedSearchSelectFilters({ selectedFilters, onFilterSelect }: Props) {
    const [filterField, setFilterField] = useState<null | string>(null);

    const onFilterFieldSelect = (value) => {
        setFilterField(value.value);
    };

    const onSelectValueFromModal = (values) => {
        if (!filterField) return;

        const newFilter: FacetFilterInput = {
            field: filterField,
            values: values as string[],
            condition: TEXT_FIELDS.has(filterField) ? FilterOperator.Contain : FilterOperator.Equal,
        };
        onFilterSelect([...selectedFilters, newFilter]);
    };

    return { filterField, setFilterField, onFilterFieldSelect, onSelectValueFromModal };
}
