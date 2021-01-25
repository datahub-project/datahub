import * as React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import { Affix, Col, Row, Tabs, Layout } from 'antd';

import { SearchablePage } from './SearchablePage';
import { fromCollectionName, fromPathName, toCollectionName, toPathName } from '../shared/EntityTypeUtil';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';
import { SearchResults } from './SearchResults';
import { EntityType, PlatformNativeType } from '../../types.generated';
import { SearchFilters } from './SearchFilters';
import { SearchCfg } from '../../conf';
import { PageRoutes } from '../../conf/Global';

const { SEARCHABLE_ENTITY_TYPES, RESULTS_PER_PAGE } = SearchCfg;

/**
 * A dedicated search page.
 *
 * TODO: Read / write filter parameters from the URL query parameters.
 */
export const SearchPage = () => {
    const history = useHistory();
    const location = useLocation();

    const params = QueryString.parse(location.search);
    const type = params.type ? fromPathName(params.type as string) : SEARCHABLE_ENTITY_TYPES[0];
    const query = params.query ? (params.query as string) : '';
    const page = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters = location.state ? ((location.state as any).filters as Array<{ field: string; value: string }>) : [];

    const { loading, error, data } = useGetSearchResultsQuery({
        variables: {
            input: {
                type,
                query,
                start: (page - 1) * RESULTS_PER_PAGE,
                count: RESULTS_PER_PAGE,
                filters,
            },
        },
    });

    const onSearchTypeChange = (newType: string) => {
        const entityType = fromCollectionName(newType);
        history.push({
            pathname: PageRoutes.SEARCH,
            search: `?type=${toPathName(entityType)}&query=${query}&page=1`,
        });
    };

    const onFilterSelect = (selected: boolean, field: string, value: string) => {
        const newFilters = selected
            ? [...filters, { field, value }]
            : filters.filter((filter) => filter.field !== field || filter.value !== value);

        history.push({
            pathname: PageRoutes.SEARCH,
            search: `?type=${toPathName(type)}&query=${query}&page=1`,
            state: {
                filters: newFilters,
            },
        });
    };

    const onResultsPageChange = (newPage: number) => {
        return history.push({
            pathname: PageRoutes.SEARCH,
            search: `?type=${toPathName(type)}&query=${query}&page=${newPage}`,
            state: {
                filters: [...filters],
            },
        });
    };

    const navigateToDataset = (urn: string) => {
        return history.push({
            pathname: `${PageRoutes.DATASETS}/${urn}`,
        });
    };

    const toDatasetSearchResult = (dataset: {
        urn: string;
        name: string;
        origin: string;
        description: string;
        platformNativeType: PlatformNativeType;
    }) => {
        return {
            title: (
                <div style={{ margin: '5px 0px 5px 2px', fontSize: '20px', fontWeight: 'bold' }}>{dataset.name}</div>
            ),
            preview: (
                <>
                    <div style={{ margin: '0px 0px 15px 0px' }}>{dataset.description}</div>
                    <div
                        style={{
                            width: '150px',
                            margin: '5px 0px 5px 0px',
                            display: 'flex',
                            justifyContent: 'space-between',
                        }}
                    >
                        <b style={{ justifySelf: 'left' }}>Data Origin</b>
                        <div style={{ justifySelf: 'right' }}>{dataset.origin}</div>
                    </div>
                    <div
                        style={{
                            width: '150px',
                            margin: '5px 0px 5px 0px',
                            display: 'flex',
                            justifyContent: 'space-between',
                        }}
                    >
                        <b>Platform</b>
                        <div>{dataset.platformNativeType}</div>
                    </div>
                </>
            ),
            onNavigate: () => navigateToDataset(dataset.urn),
        };
    };

    const toSearchResults = (elements: any) => {
        switch (type) {
            case EntityType.Dataset:
                return elements.map((element: any) => toDatasetSearchResult(element));
            default:
                throw new Error(`Search for entity of type ${type} currently not supported!`);
        }
    };

    const searchResults = (data && data?.search && toSearchResults(data.search.elements)) || [];

    return (
        <SearchablePage initialQuery={query} initialType={type}>
            <Layout.Content style={{ backgroundColor: 'white' }}>
                <Affix offsetTop={64}>
                    <Tabs
                        tabBarStyle={{ backgroundColor: 'white', padding: '0px 165px', marginBottom: '0px' }}
                        defaultActiveKey={toCollectionName(type)}
                        size="large"
                        onChange={(newPath) => onSearchTypeChange(newPath)}
                    >
                        {SEARCHABLE_ENTITY_TYPES.map((t) => (
                            <Tabs.TabPane tab={toCollectionName(t)} key={toCollectionName(t)} />
                        ))}
                    </Tabs>
                </Affix>
                <Row style={{ width: '80%', margin: 'auto auto', backgroundColor: 'white' }}>
                    <Col style={{ margin: '24px 0px 0px 0px', padding: '0px 15px' }} span={6}>
                        <SearchFilters
                            facets={(data && data?.search && data.search.facets) || []}
                            selectedFilters={filters}
                            onFilterSelect={onFilterSelect}
                        />
                    </Col>
                    <Col style={{ margin: '24px 0px 0px 0px', padding: '0px 15px' }} span={18}>
                        {loading && <p>Loading results...</p>}
                        {error && !data && <p>Search error!</p>}
                        {data && data?.search && (
                            <SearchResults
                                typeName={toCollectionName(type)}
                                results={searchResults}
                                pageStart={data.search?.start}
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
