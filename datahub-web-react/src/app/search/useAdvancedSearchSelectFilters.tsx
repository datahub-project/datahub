/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useState } from 'react';

import { FIELDS_THAT_USE_CONTAINS_OPERATOR } from '@app/search/utils/constants';

import { FacetFilterInput, FilterOperator } from '@types';

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
            condition: FIELDS_THAT_USE_CONTAINS_OPERATOR.includes(filterField)
                ? FilterOperator.Contain
                : FilterOperator.Equal,
        };
        onFilterSelect([...selectedFilters, newFilter]);
    };

    return { filterField, setFilterField, onFilterFieldSelect, onSelectValueFromModal };
}
