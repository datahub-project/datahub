import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { FilterOperator } from '@src/types.generated';
import React, { useMemo } from 'react';
import { FilterComponentProps } from '../../types';
import useValues from '../hooks/useValues';
import useOptions from './hooks/useOptions';

export default function EntityTypeFilter({ fieldName, facetState, appliedFilters, onUpdate }: FilterComponentProps) {
    const values = useValues(appliedFilters);
    const options = useOptions(facetState, values);
    const initialValues = useMemo(() => options.filter((option) => values.includes(option.value)), [values, options]);

    const onSelectUpdate = (selectedOptions: NestedSelectOption[]) => {
        const selectedValues = selectedOptions.map((option) => option.value);
        onUpdate?.({
            filters: [
                {
                    field: fieldName,
                    condition: FilterOperator.Equal,
                    values: selectedValues,
                },
            ],
        });
    };

    return (
        <NestedSelect
            initialValues={initialValues}
            onUpdate={onSelectUpdate}
            options={options}
            isMultiSelect
            width="fit-content"
            size="sm"
            showSearch
            renderCustomOptionText={(option) => option.displayName}
            showClear
            shouldDisplayConfirmationFooter
            selectLabelProps={{ variant: 'labeled', label: 'Types' }}
        />
    );
}
