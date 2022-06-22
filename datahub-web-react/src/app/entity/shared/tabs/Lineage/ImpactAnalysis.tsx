import React, { useEffect } from 'react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { useSearchAcrossLineageQuery } from '../../../../../graphql/search.generated';
import { EntityType, FacetFilterInput, LineageDirection } from '../../../../../types.generated';
import { ENTITY_FILTER_NAME } from '../../../../search/utils/constants';
import useFilters from '../../../../search/utils/useFilters';
import { SearchCfg } from '../../../../../conf';
import analytics, { EventType } from '../../../../analytics';
import { EmbeddedListSearch } from '../../components/styled/search/EmbeddedListSearch';
import generateUseSearchResultsViaRelationshipHook from './generateUseSearchResultsViaRelationshipHook';

const ImpactAnalysisWrapper = styled.div`
    flex: 1;
`;

type Props = {
    urn: string;
    direction: LineageDirection;
};

export const ImpactAnalysis = ({ urn, direction }: Props) => {
    const location = useLocation();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = params.query ? (params.query as string) : '';
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);
    const filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .map((filter) => filter.value.toUpperCase() as EntityType);

    const { data, loading } = useSearchAcrossLineageQuery({
        variables: {
            input: {
                urn,
                direction,
                types: entityFilters,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: filtersWithoutEntities,
            },
        },
    });

    useEffect(() => {
        if (!loading) {
            analytics.event({
                type: EventType.SearchAcrossLineageResultsViewEvent,
                query,
                total: data?.searchAcrossLineage?.count || 0,
            });
        }
    }, [query, data, loading]);

    return (
        <ImpactAnalysisWrapper>
            <EmbeddedListSearch
                useGetSearchResults={generateUseSearchResultsViaRelationshipHook({
                    urn,
                    direction,
                })}
                defaultShowFilters
                defaultFilters={[{ field: 'degree', value: '1' }]}
            />
        </ImpactAnalysisWrapper>
    );
};
