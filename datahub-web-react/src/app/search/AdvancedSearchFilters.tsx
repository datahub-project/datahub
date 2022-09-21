// import { PlusOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import * as React from 'react';
import { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata, SearchCondition } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { AdvancedSearchFilter } from './AdvancedSearchFilter';
import { AdvancedSearchFilterOverallUnionTypeSelect } from './AdvancedSearchFilterOverallUnionTypeSelect';
import { SelectFilterValueModal } from './SelectFilterValueModal';
import { FIELDS_WHO_USE_CONTAINS_OPERATOR, FIELD_TO_LABEL, UnionType } from './utils/constants';

export const SearchFilterWrapper = styled.div`
    min-height: 100%;
    overflow: auto;
    margin-top: 6px;
    margin-left: 12px;
    margin-right: 12px;

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

const EmptyStateSection = styled.div`
    border-radius: 5px;
    background-color: ${ANTD_GRAY[2]};
    padding: 22px;
    margin-top: 10px;
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

    return (
        <SearchFilterWrapper>
            <Select
                value="+"
                style={{ width: 67, padding: 6, fontSize: 25, fontWeight: 500 }}
                onChange={onFilterFieldSelect}
                dropdownMatchSelectWidth={false}
                filterOption={(_, option) => option?.value === 'null'}
            >
                {Object.keys(FIELD_TO_LABEL)
                    .sort((a, b) => FIELD_TO_LABEL[a].localeCompare(FIELD_TO_LABEL[b]))
                    .map((key) => (
                        <Option
                            disabled={key === 'entity' && !!selectedFilters.find((filter) => filter.field === 'entity')}
                            value={key}
                        >
                            {FIELD_TO_LABEL[key]}
                        </Option>
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
                            condition:
                                FIELDS_WHO_USE_CONTAINS_OPERATOR.indexOf(filterField) > -1
                                    ? SearchCondition.Contain
                                    : SearchCondition.Equal,
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
            {selectedFilters?.length === 0 && <EmptyStateSection>No filters applied, add one above.</EmptyStateSection>}
        </SearchFilterWrapper>
    );
};
