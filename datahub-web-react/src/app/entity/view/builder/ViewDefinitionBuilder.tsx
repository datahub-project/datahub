import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { useGetEntitiesLazyQuery } from '../../../../graphql/entity.generated';
import { Entity, FacetFilter, FacetFilterInput, LogicalOperator } from '../../../../types.generated';
import { AdvancedSearchFilters, LayoutDirection } from '../../../search/AdvancedSearchFilters';
import { ENTITY_FILTER_NAME } from '../../../search/utils/constants';
import { ANTD_GRAY } from '../../shared/constants';
import { ViewBuilderState } from '../types';
import { ViewBuilderMode } from './types';
import {
    buildEntityCache,
    extractEntityTypesFilterValues,
    fromUnionType,
    isResolutionRequired,
    toUnionType,
} from './utils';

const Container = styled.div`
    border-radius: 4px;
    padding: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    border: 1px solid ${ANTD_GRAY[4]};
    margin-bottom: 20px;
`;

/**
 * Filter fields representing entity URN criteria.
 */
const FIELDS_FOR_ENTITY_RESOLUTION = [
    'tags',
    'domains',
    'glossaryTerms',
    'owners',
    'container',
    'platform',
    'fieldGlossaryTerms',
    'fieldTags',
];

type Props = {
    mode: ViewBuilderMode;
    state: ViewBuilderState;
    updateState: (newState: ViewBuilderState) => void;
};

export const ViewDefinitionBuilder = ({ mode, state, updateState }: Props) => {
    // Stores an URN to the resolved entity.
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());

    // Find the filters requiring entity resolution.
    const filtersToResolve = useMemo(
        () =>
            state.definition?.filter?.filters?.filter((filter) =>
                FIELDS_FOR_ENTITY_RESOLUTION.includes(filter.field),
            ) || [],
        [state],
    );

    // Create an array of all urns requiring resolution
    const urnsToResolve: string[] = useMemo(() => {
        return filtersToResolve.flatMap((filter) => {
            return filter.values;
        });
    }, [filtersToResolve]);

    /**
     * Bootstrap by resolving all URNs that are not in the cache yet.
     */
    const [getEntities, { data: resolvedEntitiesData }] = useGetEntitiesLazyQuery();

    useEffect(() => {
        if (isResolutionRequired(urnsToResolve, entityCache)) {
            getEntities({ variables: { urns: urnsToResolve } });
        }
    }, [urnsToResolve, entityCache, getEntities]);

    /**
     * If some entities need to be resolved, simply build the cache from them.
     *
     * This should only happen once at component bootstrap. Typically
     * all URNs will be missing from the cache.
     */
    useEffect(() => {
        if (resolvedEntitiesData && resolvedEntitiesData.entities?.length) {
            const entities: Entity[] = (resolvedEntitiesData?.entities as Entity[]) || [];
            setEntityCache(buildEntityCache(entities));
        }
    }, [resolvedEntitiesData]);

    // Resolve "no-op" entity aggregations --> TODO: Migrate Advanced Search away from using aggregations.
    const facets = filtersToResolve.map((filter) => ({
        field: filter.field,
        aggregations: filter.values
            .filter((value) => entityCache.has(value))
            .map((value) => ({
                value,
                count: 0,
                entity: entityCache.get(value),
            })),
    }));

    const operatorType = state.definition?.filter?.operator || LogicalOperator.Or;
    const selectedFilters = state.definition?.filter?.filters || [];
    const entityTypeFilter = state?.definition?.entityTypes?.length && {
        field: ENTITY_FILTER_NAME,
        values: state?.definition?.entityTypes,
    };
    const finalFilters = (entityTypeFilter && [entityTypeFilter, ...selectedFilters]) || selectedFilters;

    const updateOperatorType = (newOperatorType) => {
        const newDefinition = {
            ...state.definition,
            filter: {
                operator: newOperatorType,
                filters: state.definition?.filter?.filters || [],
            },
        };
        updateState({
            ...state,
            definition: newDefinition,
        });
    };

    const updateFilters = (newFilters: Array<FacetFilterInput>) => {
        const entityTypes = extractEntityTypesFilterValues(newFilters);
        const filteredFilters = newFilters.filter((filter) => filter.field !== ENTITY_FILTER_NAME);
        const newDefinition = {
            entityTypes,
            filter: {
                operator: operatorType,
                filters: (filteredFilters.length > 0 ? filteredFilters : []) as FacetFilter[],
            },
        };
        updateState({
            ...state,
            definition: newDefinition,
        });
    };

    return (
        <Container>
            <AdvancedSearchFilters
                selectedFilters={finalFilters}
                facets={facets}
                onFilterSelect={updateFilters}
                onChangeUnionType={(unionType) => updateOperatorType(fromUnionType(unionType))}
                unionType={toUnionType(operatorType)}
                loading={false}
                direction={LayoutDirection.Horizontal}
                disabled={mode === ViewBuilderMode.PREVIEW}
            />
        </Container>
    );
};
