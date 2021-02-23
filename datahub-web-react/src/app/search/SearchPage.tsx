import React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Affix, Col, Row, Tabs, Layout, List, Alert } from 'antd';

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

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const selectedType = entityRegistry.getTypeOrDefaultFromPathName(
        useParams<SearchPageParams>().type || '',
        undefined,
    );
    const activeType = selectedType || entityRegistry.getDefaultSearchEntityType();
    const query: string = params.query ? (params.query as string) : '';
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);

    const { loading, error, data } = useGetSearchResultsQuery({
        variables: {
            input: {
                type: activeType,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters,
            },
        },
    });

    if (loading) {
        return <Alert type="info" message="Loading" />;
    }

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const onSearchTypeChange = (newType: string) => {
        const entityType = entityRegistry.getTypeFromCollectionName(newType);
        navigateToSearchUrl({ type: entityType, query, page: 1, history, entityRegistry });
    };

    const onFilterSelect = (selected: boolean, field: string, value: string) => {
        const newFilters = selected
            ? [...filters, { field, value }]
            : filters.filter((filter) => filter.field !== field || filter.value !== value);

        navigateToSearchUrl({ type: activeType, query, page: 1, filters: newFilters, history, entityRegistry });
    };

    const onResultsPageChange = (newPage: number) => {
        navigateToSearchUrl({ type: activeType, query, page: newPage, filters, history, entityRegistry });
    };

    const toSearchResults = (elements: any) => (
        <List
            dataSource={elements}
            renderItem={(item) => <List.Item>{entityRegistry.renderSearchResult(activeType, item)}</List.Item>}
            bordered
        />
    );

    const searchResults = toSearchResults(data?.search?.entities || []);

    return (
        <SearchablePage initialQuery={query} selectedType={selectedType}>
            <Layout.Content style={{ backgroundColor: 'white' }}>
                <Affix offsetTop={64}>
                    <Tabs
                        tabBarStyle={{ backgroundColor: 'white', padding: '0px 165px', marginBottom: '0px' }}
                        activeKey={entityRegistry.getCollectionName(activeType)}
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
                        {data?.search && (
                            <SearchResults
                                typeName={entityRegistry.getCollectionName(activeType)}
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
