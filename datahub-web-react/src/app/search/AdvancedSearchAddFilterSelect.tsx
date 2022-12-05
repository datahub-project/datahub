import { Select } from 'antd';
import * as React from 'react';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';

import { FacetFilterInput } from '../../types.generated';
import { FIELD_TO_LABEL } from './utils/constants';

const StyledPlus = styled(PlusOutlined)`
    margin-right: 6px;
`;

interface Props {
    selectedFilters: Array<FacetFilterInput>;
    onFilterFieldSelect: (value) => void;
}

const { Option } = Select;

export const AdvancedSearchAddFilterSelect = ({ selectedFilters, onFilterFieldSelect }: Props) => {
    return (
        <Select
            value={{
                value: 'value',
                label: (
                    <div>
                        <StyledPlus />
                        Add Filter
                    </div>
                ),
            }}
            labelInValue
            style={{ padding: 6, fontWeight: 500 }}
            onChange={onFilterFieldSelect}
            dropdownMatchSelectWidth={false}
            filterOption={(_, option) => option?.value === 'null'}
        >
            {Object.keys(FIELD_TO_LABEL)
                .sort((a, b) => FIELD_TO_LABEL[a].localeCompare(FIELD_TO_LABEL[b]))
                .filter((key) => key !== 'degree')
                .map((key) => (
                    <Option
                        // disable the `entity` option if they already have an entity filter selected
                        disabled={key === 'entity' && !!selectedFilters.find((filter) => filter.field === 'entity')}
                        value={key}
                        data-testid={`adv-search-add-filter-${key}`}
                    >
                        {FIELD_TO_LABEL[key]}
                    </Option>
                ))}
        </Select>
    );
};
