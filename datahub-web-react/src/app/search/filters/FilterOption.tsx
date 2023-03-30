import { Checkbox } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SearchFilterLabel } from '../SearchFilterLabel';
import { FilterFields } from './types';
import { isFilterOptionSelected } from './utils';

const FilterOptionWrapper = styled.div`
    label {
        padding: 5px 12px;
        width: 100%;
        height: 100%;
    }
`;

interface Props {
    filterFields: FilterFields;
    selectedFilterValues: string[];
    setSelectedFilterValues: (filterValues: string[]) => void;
}

export default function FilterOption({ filterFields, selectedFilterValues, setSelectedFilterValues }: Props) {
    const { field, value, count, entity } = filterFields;
    function updateFilterValues() {
        if (isFilterOptionSelected(selectedFilterValues, value)) {
            setSelectedFilterValues(selectedFilterValues.filter((value) => value !== value));
        } else {
            setSelectedFilterValues([...selectedFilterValues, value]);
        }
    }

    return (
        <FilterOptionWrapper>
            <Checkbox checked={isFilterOptionSelected(selectedFilterValues, value)} onClick={updateFilterValues}>
                <SearchFilterLabel field={field} value={value} count={count} entity={entity} />
            </Checkbox>
        </FilterOptionWrapper>
    );
}
