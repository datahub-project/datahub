import { Checkbox } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { AggregationMetadata } from '../../../types.generated';
import { SearchFilterLabel } from '../SearchFilterLabel';
import { isFilterOptionSelected } from './utils';

const FilterOptionWrapper = styled.div`
    label {
        padding: 5px 12px;
        width: 100%;
        height: 100%;
    }
`;

interface Props {
    filterField: string;
    aggregation: AggregationMetadata;
    selectedFilterValues: string[];
    setSelectedFilterValues: (filterValues: string[]) => void;
}

export default function FilterOption({
    filterField,
    aggregation,
    selectedFilterValues,
    setSelectedFilterValues,
}: Props) {
    function updateFilterValues() {
        if (isFilterOptionSelected(selectedFilterValues, aggregation.value)) {
            setSelectedFilterValues(selectedFilterValues.filter((value) => value !== aggregation.value));
        } else {
            setSelectedFilterValues([...selectedFilterValues, aggregation.value]);
        }
    }

    return (
        <FilterOptionWrapper>
            <Checkbox
                checked={isFilterOptionSelected(selectedFilterValues, aggregation.value)}
                onClick={updateFilterValues}
            >
                <SearchFilterLabel
                    field={filterField}
                    value={aggregation.value}
                    count={aggregation.count}
                    entity={aggregation.entity}
                />
            </Checkbox>
        </FilterOptionWrapper>
    );
}
