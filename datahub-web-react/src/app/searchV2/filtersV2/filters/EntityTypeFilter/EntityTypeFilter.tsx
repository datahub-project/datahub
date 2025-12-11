/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import useOptions from '@app/searchV2/filtersV2/filters/EntityTypeFilter/hooks/useOptions';
import useValues from '@app/searchV2/filtersV2/filters/hooks/useValues';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { FilterOperator } from '@src/types.generated';

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
            dataTestId="filter-entity-type"
            size="sm"
            showSearch
            renderCustomOptionText={(option) => option.displayName}
            showClear
            shouldDisplayConfirmationFooter
            selectLabelProps={{ variant: 'labeled', label: 'Types' }}
        />
    );
}
