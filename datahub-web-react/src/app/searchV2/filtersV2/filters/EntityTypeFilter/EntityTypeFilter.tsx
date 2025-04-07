import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { FilterOperator } from '@src/types.generated';
import React, { useMemo } from 'react';
import { FilterComponentProps } from '../../types';
import useValues from '../hooks/useValues';
import useOptions from './hooks/useOptions';
import { entityTypeFilteringPredicate } from './utils';

const LABEL_RENDERER = (entityTypeName: string) => <>{entityTypeName}</>;

export default function EntityTypeFilter({ fieldName, facetState, appliedFilters, onUpdate }: FilterComponentProps) {
    const values = useValues(appliedFilters);
    const options = useOptions(facetState, values, LABEL_RENDERER);
    const initialValues = useMemo(() => options.filter((option) => values.includes(option.value)), [values, options]);

    const onSelectUpdate = (selectedOptions: SelectOption[]) => {
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
            shouldFilterOptions
            filteringPredicate={entityTypeFilteringPredicate}
            showCount
            shouldManuallyUpdate
            selectLabelProps={{ variant: 'labeled', label: 'Types' }}
        />
    );
}
