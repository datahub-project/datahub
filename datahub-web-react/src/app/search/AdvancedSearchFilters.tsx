import { PlusOutlined } from '@ant-design/icons';
import { Button, Select } from 'antd';
import * as React from 'react';
import { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata, SearchCondition } from '../../types.generated';
import { AdvancedSearchFilter } from './AdvancedSearchFilter';
import { SelectFilterValueModal } from './SelectFilterValueModal';
import { FIELD_TO_LABEL } from './utils/constants';

export const SearchFilterWrapper = styled.div`
    min-height: 100%;
    overflow: auto;

    &::-webkit-scrollbar {
        height: 12px;
        width: 1px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;

interface Props {
    selectedFilters: Array<FacetFilterInput>;
    facets: Array<FacetMetadata>;
    onFilterSelect: (newFilters: Array<FacetFilterInput>) => void;
}

const { Option } = Select;

export const AdvancedSearchFilters = ({ facets, selectedFilters, onFilterSelect }: Props) => {
    console.log(onFilterSelect);
    const [filterField, setFilterField] = useState<null | string>(null);

    const onFilterFieldSelect = (value) => {
        setFilterField(value);
    };

    return (
        <SearchFilterWrapper>
            <Select
                value={filterField || 'null'}
                style={{ width: 120 }}
                bordered={false}
                onChange={onFilterFieldSelect}
            >
                <Option value="null">
                    <Button type="text" style={{ padding: 0 }}>
                        <PlusOutlined />
                        <span>Add Filter</span>
                    </Button>
                </Option>
                {Object.keys(FIELD_TO_LABEL).map((key) => (
                    <Option value={key}>{FIELD_TO_LABEL[key]}</Option>
                ))}
            </Select>
            {selectedFilters.map((filter) => (
                <AdvancedSearchFilter
                    facet={facets.find((facet) => facet.field === filter.field) || facets[0]}
                    filter={filter}
                    onClose={() => {
                        onFilterSelect(selectedFilters.filter((f) => f !== filter));
                    }}
                    onUpdate={(newValue) => {
                        onFilterSelect(
                            selectedFilters.map((f) => {
                                if (f === filter) {
                                    return newValue;
                                }
                                return f;
                            }),
                        );
                    }}
                />
            ))}
            {filterField && (
                <SelectFilterValueModal
                    facet={facets.find((facet) => facet.field === filterField) || facets[0]}
                    onCloseModal={() => setFilterField(null)}
                    filterField={filterField}
                    onSelect={(values) => {
                        const newFilter: FacetFilterInput = {
                            field: filterField,
                            values: values as string[],
                            condition: SearchCondition.Contain,
                        };
                        onFilterSelect([...selectedFilters, newFilter]);
                    }}
                />
            )}
        </SearchFilterWrapper>
    );
};
