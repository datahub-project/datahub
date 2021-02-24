import React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Affix, Tabs, Space } from 'antd';
import { SearchablePage } from './SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput } from '../../types.generated';
import useFilters from './utils/useFilters';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { EntitySearchResults } from './EntitySearchResults';
import { IconStyleType } from '../entity/Entity';
import { EntityGroupSearchResults } from './EntityGroupSearchResults';

const ALL_ENTITIES_TAB_NAME = 'All';

type SearchPageParams = {
    type?: string;
};

/**
 * A search results page.
 */
export const SearchPage = () => {
    const history = useHistory();
    const location = useLocation();

    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const query: string = params.query ? (params.query as string) : '';
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);

    const onSearch = (q: string) => {
        navigateToSearchUrl({ type: activeType, query: q, page: 1, history, entityRegistry });
    };

    const onSearchTypeChange = (newType: string) => {
        const entityType = entityRegistry.getTypeFromCollectionName(newType);
        navigateToSearchUrl({ type: entityType, query, page: 1, history, entityRegistry });
    };

    const onAddFilters = () => {
        // TODO.
        return null;
    };

    // const onFilterSelect = (selected: boolean, field: string, value: string) => {
    //     const newFilters = selected
    //         ? [...filters, { field, value }]
    //         : filters.filter((filter) => filter.field !== field || filter.value !== value);

    //     navigateToSearchUrl({ type: activeType, query, page: 1, filters: newFilters, history, entityRegistry });
    // };

    const onResultsPageChange = (newPage: number) => {
        navigateToSearchUrl({ type: activeType, query, page: newPage, filters, history, entityRegistry });
    };

    return (
        <SearchablePage initialQuery={query} onSearch={onSearch}>
            <Affix offsetTop={80} style={{ backgroundColor: '#FFFFFF' }}>
                <Tabs
                    tabBarStyle={{
                        backgroundColor: '#FFFFFF',
                        paddingLeft: '165px',
                        paddingTop: '12px',
                        color: 'gray',
                    }}
                    activeKey={activeType ? entityRegistry.getCollectionName(activeType) : ALL_ENTITIES_TAB_NAME}
                    size="large"
                    onChange={onSearchTypeChange}
                >
                    <Tabs.TabPane tab={<span style={{ fontSize: '20px' }}>All</span>} key={ALL_ENTITIES_TAB_NAME} />
                    {searchTypes.map((t) => (
                        <Tabs.TabPane
                            tab={
                                <span style={{ display: 'flex', alignItems: 'center' }}>
                                    {entityRegistry.getIcon(t, 16, IconStyleType.TAB_VIEW)}{' '}
                                    <span style={{ fontSize: '20px' }}>{entityRegistry.getCollectionName(t)}</span>
                                </span>
                            }
                            key={entityRegistry.getCollectionName(t)}
                        />
                    ))}
                </Tabs>
            </Affix>
            <Space
                direction="vertical"
                style={{ width: '100%', paddingRight: '10%', paddingLeft: '10%', paddingTop: '20px' }}
            >
                {activeType ? (
                    <EntitySearchResults
                        type={activeType}
                        page={page}
                        query={query}
                        filters={filters}
                        onAddFilters={onAddFilters}
                        onChangePage={onResultsPageChange}
                    />
                ) : (
                    entityRegistry
                        .getSearchEntityTypes()
                        .map((entityType) => <EntityGroupSearchResults type={entityType} query={query} />)
                )}
            </Space>
        </SearchablePage>
    );
};
