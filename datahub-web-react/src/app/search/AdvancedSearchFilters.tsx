import * as React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { AdvancedSearchFilter } from './AdvancedSearchFilter';
import { AdvancedSearchFilterOverallUnionTypeSelect } from './AdvancedSearchFilterOverallUnionTypeSelect';
import { AdvancedFilterSelectValueModal } from './AdvancedFilterSelectValueModal';
import { UnionType } from './utils/constants';
import { AdvancedSearchAddFilterSelect } from './AdvancedSearchAddFilterSelect';
import useAdvancedSearchSelectFilters from './useAdvancedSearchSelectFilters';

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

const AdvancedSearchFiltersGroup = styled.div`
    display: flex;
    flex-wrap: wrap;
`;

export enum LayoutDirection {
    Horizontal = 'horizontal',
    Vertical = 'vertical',
}

interface Props {
    selectedFilters: Array<FacetFilterInput>;
    facets: Array<FacetMetadata>;
    onFilterSelect: (newFilters: Array<FacetFilterInput>) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    unionType?: UnionType;
    loading: boolean;
    direction?: LayoutDirection;
    disabled?: boolean;
}

export const AdvancedSearchFilters = ({
    unionType = UnionType.AND,
    facets,
    selectedFilters,
    onFilterSelect,
    onChangeUnionType,
    loading,
    direction = LayoutDirection.Vertical,
    disabled = false,
}: Props) => {
    const { filterField, setFilterField, onFilterFieldSelect, onSelectValueFromModal } = useAdvancedSearchSelectFilters(
        { selectedFilters, onFilterSelect },
    );

    return (
        <>
            {!disabled && (
                <AdvancedSearchAddFilterSelect
                    selectedFilters={selectedFilters}
                    onFilterFieldSelect={onFilterFieldSelect}
                />
            )}
            <AdvancedSearchFiltersGroup>
                {selectedFilters.map((filter) => (
                    <AdvancedSearchFilter
                        key={`${filter.field}-${filter.condition}-${filter.negated}-${filter.values}-${filter.value}`}
                        facet={facets.find((facet) => facet.field === filter.field) || facets[0]}
                        loading={loading}
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
                        disabled={disabled}
                    />
                ))}
            </AdvancedSearchFiltersGroup>
            {filterField && (
                <AdvancedFilterSelectValueModal
                    facet={facets.find((facet) => facet.field === filterField) || null}
                    onCloseModal={() => setFilterField(null)}
                    filterField={filterField}
                    onSelect={onSelectValueFromModal}
                />
            )}
            {selectedFilters?.length >= 2 && (
                <AnyAllSection>
                    Show results that match{' '}
                    <AdvancedSearchFilterOverallUnionTypeSelect
                        unionType={unionType}
                        onUpdate={(newValue) => onChangeUnionType(newValue)}
                        disabled={disabled}
                    />
                </AnyAllSection>
            )}
            {selectedFilters?.length === 0 && direction === LayoutDirection.Vertical && (
                <EmptyStateSection>No filters applied.</EmptyStateSection>
            )}
        </>
    );
};
