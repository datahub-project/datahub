// import { PlusOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import * as React from 'react';
import { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata, SearchCondition } from '../../types.generated';
import { AdvancedSearchFilter } from './AdvancedSearchFilter';
import { AdvancedSearchFilterOverallUnionTypeSelect } from './AdvancedSearchFilterOverallUnionTypeSelect';
import { SelectFilterValueModal } from './SelectFilterValueModal';
import { FIELD_TO_LABEL, UnionType } from './utils/constants';

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

const AnyAllSection = styled.div`
    padding: 6px;
`;

interface Props {
    selectedFilters: Array<FacetFilterInput>;
    facets: Array<FacetMetadata>;
    onFilterSelect: (newFilters: Array<FacetFilterInput>) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    unionType?: UnionType;
}

const { Option } = Select;

export const AdvancedSearchFilters = ({
    unionType = UnionType.AND,
    facets,
    selectedFilters,
    onFilterSelect,
    onChangeUnionType,
}: Props) => {
    const [filterField, setFilterField] = useState<null | string>(null);

    const onFilterFieldSelect = (value) => {
        setFilterField(value);
    };

    console.log({ facets });

    return (
        <SearchFilterWrapper>
            <Select
                // size="small"
                value={filterField || '+'}
                style={{ width: 67, padding: 6, fontSize: 25, fontWeight: 500 }}
                onChange={onFilterFieldSelect}
                dropdownMatchSelectWidth={false}
                filterOption={(_, option) => option?.value === 'null'}
            >
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
                    facet={facets.find((facet) => facet.field === filterField) || null}
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
            {selectedFilters?.length > 0 && (
                <AnyAllSection>
                    Show results that match{' '}
                    <AdvancedSearchFilterOverallUnionTypeSelect
                        unionType={unionType}
                        onUpdate={(newValue) => onChangeUnionType(newValue)}
                    />
                </AnyAllSection>
            )}
        </SearchFilterWrapper>
    );
};
