import React, { useState } from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { message } from 'antd';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, FacetFilterInput } from '../../../../../../types.generated';
import useFilters from '../../../../../search/utils/useFilters';
import { ENTITY_FILTER_NAME } from '../../../../../search/utils/constants';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../graphql/search.generated';
import { SearchCfg } from '../../../../../../conf';
import { navigateToEntitySearchUrl } from './navigateToEntitySearchUrl';
import { EmbeddedListSearchResults } from './EmbeddedListSearchResults';
import EmbeddedListSearchHeader from './EmbeddedListSearchHeader';

type SearchPageParams = {
    type?: string;
};

type Props = {
    emptySearchQuery?: string | null;
    fixedFilter?: FacetFilterInput | null;
    placeholderText?: string | null;
};

export const EmbeddedListSearch = ({ emptySearchQuery, fixedFilter, placeholderText }: Props) => {
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = params.query ? (params.query as string) : '';
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);
    const filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );
    const finalFilters = (fixedFilter && [...filtersWithoutEntities, fixedFilter]) || filtersWithoutEntities;
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .map((filter) => filter.value.toUpperCase() as EntityType);

    const [showFilters, setShowFilters] = useState(false);

    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: entityFilters,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: finalFilters,
            },
        },
    });

    const onSearch = (q: string) => {
        let finalQuery = q;
        if (q.trim().length === 0) {
            if (emptySearchQuery) {
                finalQuery = emptySearchQuery;
            } else {
                return;
            }
        }
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            type: activeType,
            query: finalQuery,
            page: 1,
            history,
        });
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            type: activeType,
            query,
            page: 1,
            filters: newFilters,
            history,
        });
    };

    const onChangePage = (newPage: number) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            type: activeType,
            query,
            page: newPage,
            filters,
            history,
        });
    };

    const toggleFilters = () => {
        setShowFilters(!showFilters);
    };

    // Filter out the persistent filter values
    const filteredFilters =
        data?.searchAcrossEntities?.facets?.filter((facet) => facet.field !== fixedFilter?.field) || [];

    return (
        <>
            {error && message.error(`Failed to complete search: ${error && error.message}`)}
            <EmbeddedListSearchHeader
                onSearch={onSearch}
                placeholderText={placeholderText}
                onToggleFilters={toggleFilters}
            />
            <EmbeddedListSearchResults
                loading={loading}
                searchResponse={data?.searchAcrossEntities}
                filters={filteredFilters}
                selectedFilters={filters}
                onChangeFilters={onChangeFilters}
                onChangePage={onChangePage}
                page={page}
                showFilters={showFilters}
            />
        </>
    );
};
