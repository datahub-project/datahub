import React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Affix, Col, Row, Tabs, Layout } from 'antd';
import { SearchablePage } from './SearchablePage';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';
import { SearchResults } from './SearchResults';
import { SearchFilters } from './SearchFilters';
import { SearchCfg } from '../../conf';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput } from '../../types.generated';
import useFilters from './utils/useFilters';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';

type SearchPageParams = {
    type?: string;
};

/**
 * A dedicated search page.
 *
 * TODO: Read / write filter parameters from / to the URL query parameters.
 */
export const SearchPage = () => {
    const history = useHistory();
    const location = useLocation();

    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();

    const params = QueryString.parse(location.search);
    const type = entityRegistry.getTypeOrDefaultFromPathName(
        useParams<SearchPageParams>().type || '',
        entityRegistry.getDefaultSearchEntityType(),
    );
    const query: string = params.query ? (params.query as string) : '';
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);

    const { loading, error, data } = useGetSearchResultsQuery({
        variables: {
            input: {
                type,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters,
            },
        },
    });

    const onSearchTypeChange = (newType: string) => {
        const entityType = entityRegistry.getTypeFromCollectionName(newType);
        navigateToSearchUrl({ type: entityType, query, page: 1, history, entityRegistry });
    };

    const onFilterSelect = (selected: boolean, field: string, value: string) => {
        const newFilters = selected
            ? [...filters, { field, value }]
            : filters.filter((filter) => filter.field !== field || filter.value !== value);

        navigateToSearchUrl({ type, query, page: 1, filters: newFilters, history, entityRegistry });
    };

    const onResultsPageChange = (newPage: number) => {
        navigateToSearchUrl({ type, query, page: newPage, filters, history, entityRegistry });
    };

    const toSearchResults = (elements: any) => {
        return elements.map((element: any) => entityRegistry.renderSearchResult(type, element));
    };

    const searchResults = toSearchResults(data?.search?.elements || []);

    return (
        <SearchablePage initialQuery={query} initialType={type}>
            <Layout.Content style={{ backgroundColor: 'white' }}>
                <Affix offsetTop={64}>
                    <Tabs
                        tabBarStyle={{ backgroundColor: 'white', padding: '0px 165px', marginBottom: '0px' }}
                        activeKey={entityRegistry.getCollectionName(type)}
                        size="large"
                        onChange={onSearchTypeChange}
                    >
                        {searchTypes.map((t) => (
                            <Tabs.TabPane
                                tab={entityRegistry.getCollectionName(t)}
                                key={entityRegistry.getCollectionName(t)}
                            />
                        ))}
                    </Tabs>
                </Affix>
                <Row style={{ width: '80%', margin: 'auto auto', backgroundColor: 'white' }}>
                    <Col style={{ margin: '24px 0px 0px 0px', padding: '0px 16px' }} span={6}>
                        <SearchFilters
                            facets={data?.search?.facets || []}
                            selectedFilters={filters}
                            onFilterSelect={onFilterSelect}
                        />
                    </Col>
                    <Col style={{ margin: '24px 0px 0px 0px', padding: '0px 16px' }} span={18}>
                        {loading && <p>Loading results...</p>}
                        {error && !data && <p>Search error!</p>}
                        {data?.search && (
                            <SearchResults
                                typeName={entityRegistry.getCollectionName(type)}
                                results={searchResults}
                                pageStart={data?.search?.start}
                                pageSize={data.search?.count}
                                totalResults={data.search?.total}
                                onChangePage={onResultsPageChange}
                            />
                        )}
                    </Col>
                </Row>
            </Layout.Content>
        </SearchablePage>
    );
};
