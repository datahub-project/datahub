import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { AdvancedFilterSelectValueModal } from '../AdvancedFilterSelectValueModal';
import { AdvancedSearchAddFilterSelect } from '../AdvancedSearchAddFilterSelect';
import { AdvancedSearchFilter } from '../AdvancedSearchFilter';
import { AdvancedSearchFilterOverallUnionTypeSelect } from '../AdvancedSearchFilterOverallUnionTypeSelect';
import useAdvancedSearchSelectFilters from '../useAdvancedSearchSelectFilters';
import { UnionType } from '../utils/constants';
import { FlexSpacer, FlexWrapper, TextButton } from './BasicFilters';

const AnyAllToggle = styled.div`
    font-weight: 700;
    margin-bottom: 8px;
`;

interface Props {
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onlyShowAdvancedFilters: boolean;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    showBasicFilters: () => void;
}

export default function AdvancedFilters({
    availableFilters,
    activeFilters,
    unionType,
    onlyShowAdvancedFilters,
    onChangeFilters,
    showBasicFilters,
    onChangeUnionType,
}: Props) {
    const { filterField, setFilterField, onFilterFieldSelect, onSelectValueFromModal } = useAdvancedSearchSelectFilters(
        { selectedFilters: activeFilters, onFilterSelect: onChangeFilters },
    );

    return (
        <>
            {activeFilters?.length >= 2 && (
                <AnyAllToggle>
                    Show results that match{' '}
                    <AdvancedSearchFilterOverallUnionTypeSelect
                        unionType={unionType}
                        onUpdate={(newValue) => onChangeUnionType(newValue)}
                    />
                </AnyAllToggle>
            )}
            <FlexSpacer>
                <FlexWrapper>
                    <AdvancedSearchAddFilterSelect
                        selectedFilters={activeFilters}
                        onFilterFieldSelect={onFilterFieldSelect}
                    />

                    {activeFilters.map((filter) => (
                        <AdvancedSearchFilter
                            key={`${filter.field}-${filter.condition}-${filter.negated}-${filter.values}-${filter.value}`}
                            facet={
                                availableFilters.find((facet) => facet.field === filter.field) || availableFilters[0]
                            }
                            loading={false}
                            filter={filter}
                            onClose={() => {
                                onChangeFilters(activeFilters.filter((f) => f !== filter));
                            }}
                            onUpdate={(newValue) => {
                                onChangeFilters(
                                    activeFilters.map((f) => {
                                        if (f === filter) {
                                            return newValue;
                                        }
                                        return f;
                                    }),
                                );
                            }}
                            isCompact
                        />
                    ))}
                </FlexWrapper>
                <TextButton disabled={onlyShowAdvancedFilters} type="text" onClick={showBasicFilters} marginTop={0}>
                    Basic Filters
                </TextButton>
            </FlexSpacer>
            {filterField && (
                <AdvancedFilterSelectValueModal
                    facet={availableFilters.find((facet) => facet.field === filterField) || null}
                    onCloseModal={() => setFilterField(null)}
                    filterField={filterField}
                    onSelect={onSelectValueFromModal}
                />
            )}
        </>
    );
}
