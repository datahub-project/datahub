import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { fromUnionType, toUnionType } from '@app/entityV2/view/builder/utils';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { ENTITY_FILTER_NAME } from '@app/search/utils/constants';
// eslint-disable-next-line import/no-cycle
import SearchFiltersBuilder from '@app/searchV2/filters/SearchFiltersBuilder';
import { VIEW_BUILDER_FIELDS } from '@app/searchV2/filters/field/fields';
import { convertFrontendToBackendOperatorType } from '@app/searchV2/filters/operator/operator';
import { FilterPredicate } from '@app/searchV2/filters/types';
import { convertToSelectedFilterPredictes } from '@app/searchV2/filters/utils';

import { LogicalOperator } from '@types';

const Container = styled.div`
    border-radius: 4px;
    padding: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    border: 1px solid ${ANTD_GRAY[4]};
    margin-bottom: 20px;
`;

type Props = {
    mode: ViewBuilderMode;
    state: ViewBuilderState;
    updateState: (newState: ViewBuilderState) => void;
};

export const ViewDefinitionBuilder = ({ mode, state, updateState }: Props) => {
    // The selected operator type.
    const operatorType = state.definition?.filter?.operator || LogicalOperator.Or;

    // The selected filters.
    const selectedFilters = state.definition?.filter?.filters || [];

    // The union type
    const unionType = toUnionType(state?.definition?.filter?.operator || LogicalOperator.Or);

    // Parse the default entity type filter from the state.
    const entityTypeFilter = state?.definition?.entityTypes?.length && {
        field: ENTITY_FILTER_NAME,
        values: state?.definition?.entityTypes,
    };

    const filterPredicates: FilterPredicate[] = convertToSelectedFilterPredictes(
        (entityTypeFilter && [entityTypeFilter, ...selectedFilters]) || selectedFilters,
        [],
    );

    const updateFilters = (newFilters: FilterPredicate[]) => {
        const backendFilters = newFilters.map((predicate) => {
            const condition = convertFrontendToBackendOperatorType(predicate.operator);
            return {
                field: predicate.field.field,
                values: predicate.values.map((value) => value.value),
                condition: condition.operator,
                negated: condition.negated,
            };
        });
        const newDefinition = {
            entityTypes: [], // Now we use the raw entity type filters.
            filter: {
                operator: operatorType,
                filters: backendFilters || [],
            },
        };
        updateState({
            ...state,
            definition: newDefinition,
        });
    };

    const updateUnionType = (newUnionType) => {
        const newDefinition = {
            ...state.definition,
            filter: {
                operator: fromUnionType(newUnionType),
                filters: state.definition?.filter?.filters || [],
            },
        };
        updateState({
            ...state,
            definition: newDefinition,
        });
    };

    const onClearFilters = () => {
        const newDefinition = {
            ...state.definition,
            filter: {
                operator: operatorType,
                filters: [],
            },
        };
        updateState({
            ...state,
            definition: newDefinition,
        });
    };

    return (
        <Container>
            <SearchFiltersBuilder
                fields={VIEW_BUILDER_FIELDS}
                filters={filterPredicates}
                onChangeFilters={updateFilters}
                onClearFilters={onClearFilters}
                disabled={mode === ViewBuilderMode.PREVIEW}
                unionType={unionType}
                onChangeUnionType={updateUnionType}
                showUnionType
                vertical
            />
        </Container>
    );
};
