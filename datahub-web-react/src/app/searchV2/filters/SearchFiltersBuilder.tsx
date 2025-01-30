import React from 'react';
import styled from 'styled-components';
import { TextButton } from './styledComponents';
// eslint-disable-next-line import/no-cycle
import SelectedFilter from './SelectedFilter';
import { FilterField, FilterPredicate } from './types';
import AddFilterDropdown from './AddFilterDropdown';
import { useHydrateFilters } from './useHydrateFilters';
import { AdvancedSearchFilterOverallUnionTypeSelect } from '../AdvancedSearchFilterOverallUnionTypeSelect';
import { UnionType } from '../utils/constants';

const Container = styled.div``;

const HorizontalWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px;
`;

const VerticalWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

export const FlexSpacer = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const FilterButtonsWrapper = styled.div`
    display: flex;
    flex-wrap: nowrap;
`;

const AnyAllToggle = styled.div`
    margin-top: 8px;
`;

interface Props {
    fields?: FilterField[];
    filters: FilterPredicate[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FilterPredicate[]) => void;
    onChangeUnionType?: (unionType: UnionType) => void;
    onClearFilters: () => void;
    disabled?: boolean;
    vertical?: boolean;
    showUnionType?: boolean;
    showAddFilter?: boolean;
    showClearAll?: boolean;
    isCompact?: boolean;
    isOperatorDisabled?: boolean;
    includeCount?: boolean;
}

export default function SearchFiltersBuilder({
    fields,
    filters,
    onChangeFilters,
    onClearFilters,
    unionType,
    onChangeUnionType,
    vertical,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    disabled = false,
    showUnionType = false,
    showAddFilter = true,
    showClearAll = true,
    isCompact = false,
    includeCount = false,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    isOperatorDisabled = false,
}: Props) {
    const hydratedFilters = useHydrateFilters(filters);

    const onChangeFilterOperator = (index, newOperator) => {
        const newFilters = filters.map((filter, i) => {
            if (i === index) {
                return {
                    ...filter,
                    operator: newOperator,
                };
            }
            return filter;
        });
        onChangeFilters(newFilters);
    };

    const onChangeFilterValues = (index, newValues) => {
        const newFilters = filters.map((filter, i) => {
            if (i === index) {
                return {
                    ...filter,
                    values: newValues,
                };
            }
            return filter;
        });
        onChangeFilters(newFilters);
    };

    const onAddFilter = (predicate: FilterPredicate) => {
        const newFilters = [
            ...filters,
            {
                field: predicate.field,
                operator: predicate.operator,
                values: predicate.values,
                defaultValueOptions: predicate.defaultValueOptions,
            },
        ];
        onChangeFilters(newFilters);
    };

    const onRemoveFilter = (indexToRemove: number) => {
        const newFilters = filters.filter((_, index) => index !== indexToRemove);
        onChangeFilters(newFilters);
    };

    const Wrapper = vertical ? VerticalWrapper : HorizontalWrapper;

    const addFilter = <AddFilterDropdown fields={fields} onAddFilter={onAddFilter} includeCount={includeCount} />;
    return (
        <Container>
            <FlexSpacer>
                <Wrapper>
                    {showAddFilter && vertical && addFilter}
                    {hydratedFilters.map((predicate, index) => (
                        <SelectedFilter
                            predicate={predicate}
                            onChangeOperator={(operator) => onChangeFilterOperator(index, operator)}
                            onChangeValues={(newValues) => onChangeFilterValues(index, newValues)}
                            onRemoveFilter={() => onRemoveFilter(index)}
                            isCompact={isCompact}
                            // isOperatorDisabled={isOperatorDisabled}
                        />
                    ))}
                    {showAddFilter && !vertical && addFilter}
                </Wrapper>
                {showClearAll && (
                    <TextButton type="text" onClick={onClearFilters} height={14} data-testid="clear-all-filters">
                        clear all
                    </TextButton>
                )}
            </FlexSpacer>
            {showUnionType && hydratedFilters?.length >= 2 && (
                <AnyAllToggle>
                    Show results that match{' '}
                    <AdvancedSearchFilterOverallUnionTypeSelect
                        unionType={unionType}
                        onUpdate={(newValue) => onChangeUnionType?.(newValue)}
                    />
                </AnyAllToggle>
            )}
        </Container>
    );
}
