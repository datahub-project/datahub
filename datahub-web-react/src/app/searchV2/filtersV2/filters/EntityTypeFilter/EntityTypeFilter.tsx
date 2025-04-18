<<<<<<< HEAD
import React, { useMemo } from 'react';

import useOptions from '@app/searchV2/filtersV2/filters/EntityTypeFilter/hooks/useOptions';
import useValues from '@app/searchV2/filtersV2/filters/hooks/useValues';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { FilterOperator } from '@src/types.generated';
=======
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { FilterOperator } from '@src/types.generated';
import React, { useMemo } from 'react';
import { FilterComponentProps } from '../../types';
import useValues from '../hooks/useValues';
import useOptions from './hooks/useOptions';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
