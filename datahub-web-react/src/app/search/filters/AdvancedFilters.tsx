import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { AdvancedFilterSelectValueModal } from '@app/search/AdvancedFilterSelectValueModal';
import { AdvancedSearchAddFilterSelect } from '@app/search/AdvancedSearchAddFilterSelect';
import { AdvancedSearchFilter } from '@app/search/AdvancedSearchFilter';
import { AdvancedSearchFilterOverallUnionTypeSelect } from '@app/search/AdvancedSearchFilterOverallUnionTypeSelect';
import { FilterButtonsWrapper, FlexSpacer, FlexWrapper } from '@app/search/filters/BasicFilters';
import SaveViewButton from '@app/search/filters/SaveViewButton';
import { TextButton } from '@app/search/filters/styledComponents';
import useAdvancedSearchSelectFilters from '@app/search/useAdvancedSearchSelectFilters';
import { UnionType } from '@app/search/utils/constants';
import { hasAdvancedFilters } from '@app/search/utils/hasAdvancedFilters';

import { FacetFilterInput, FacetMetadata } from '@types';

const AnyAllToggle = styled.div`
    font-weight: 700;
    margin-bottom: 8px;
`;

const EmptyStateSection = styled.div`
    padding: 6px 20px;
    background-color: ${ANTD_GRAY[2]};
    border-radius: 5px;
`;

interface Props {
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    showBasicFilters: () => void;
}

export default function AdvancedFilters({
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
    showBasicFilters,
    onChangeUnionType,
}: Props) {
    const { filterField, setFilterField, onFilterFieldSelect, onSelectValueFromModal } = useAdvancedSearchSelectFilters(
        { selectedFilters: activeFilters, onFilterSelect: onChangeFilters },
    );
    const userContext = useUserContext();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;
    const showSaveViewButton = activeFilters?.length > 0 && selectedViewUrn === undefined;
    const onlyShowAdvancedFilters = hasAdvancedFilters(activeFilters, unionType);

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
                        isCompact
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
                    {!activeFilters.length && <EmptyStateSection>No filters applied.</EmptyStateSection>}
                </FlexWrapper>
                <FilterButtonsWrapper>
                    {showSaveViewButton && <SaveViewButton activeFilters={activeFilters} unionType={unionType} />}
                    <TextButton disabled={onlyShowAdvancedFilters} type="text" onClick={showBasicFilters} marginTop={0}>
                        Basic Filters
                    </TextButton>
                </FilterButtonsWrapper>
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
