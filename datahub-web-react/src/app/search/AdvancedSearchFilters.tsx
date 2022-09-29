import * as React from 'react';
import { useState } from 'react';
import styled from 'styled-components';

import { FacetFilterInput, FacetMetadata, SearchCondition } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { AdvancedSearchFilter } from './AdvancedSearchFilter';
import { AdvancedSearchFilterOverallUnionTypeSelect } from './AdvancedSearchFilterOverallUnionTypeSelect';
import { AdvancedFilterSelectValueModal } from './AdvancedFilterSelectValueModal';
import { FIELDS_THAT_USE_CONTAINS_OPERATOR, UnionType } from './utils/constants';
import { AdvancedSearchAddFilterSelect } from './AdvancedSearchAddFilterSelect';

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
    color: ${ANTD_GRAY[8]};
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

export const AdvancedSearchFilters = ({
    unionType = UnionType.AND,
    facets,
    selectedFilters,
    onFilterSelect,
    onChangeUnionType,
}: Props) => {
    const [filterField, setFilterField] = useState<null | string>(null);

    const onFilterFieldSelect = (value) => {
        setFilterField(value.value);
    };

    const onSelectValueFromModal = (values) => {
        if (!filterField) return;

        const newFilter: FacetFilterInput = {
            field: filterField,
            values: values as string[],
            value: '', // TODO(Gabe): remove once we refactor the model
            condition: FIELDS_THAT_USE_CONTAINS_OPERATOR.includes(filterField)
                ? SearchCondition.Contain
                : SearchCondition.Equal,
        };
        onFilterSelect([...selectedFilters, newFilter]);
    };

    return (
        <SearchFilterWrapper>
            <AdvancedSearchAddFilterSelect
                selectedFilters={selectedFilters}
                onFilterFieldSelect={onFilterFieldSelect}
            />
            {selectedFilters?.length >= 2 && (
                <AnyAllSection>
                    Show results that match{' '}
                    <AdvancedSearchFilterOverallUnionTypeSelect
                        unionType={unionType}
                        onUpdate={(newValue) => onChangeUnionType(newValue)}
                    />
                </AnyAllSection>
            )}
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
                <AdvancedFilterSelectValueModal
                    facet={facets.find((facet) => facet.field === filterField) || null}
                    onCloseModal={() => setFilterField(null)}
                    filterField={filterField}
                    onSelect={onSelectValueFromModal}
                />
            )}
            {selectedFilters?.length === 0 && <EmptyStateSection>No filters applied, add one above.</EmptyStateSection>}
        </SearchFilterWrapper>
    );
};
