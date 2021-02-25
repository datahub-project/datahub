import React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Affix, Tabs } from 'antd';
import { SearchablePage } from './SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput } from '../../types.generated';
import useFilters from './utils/useFilters';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { EntitySearchResults } from './EntitySearchResults';
import { IconStyleType } from '../entity/Entity';
import { EntityGroupSearchResults } from './EntityGroupSearchResults';

const ALL_ENTITIES_TAB_NAME = 'All';

const styles = {
    tabs: {
        backgroundColor: '#FFFFFF',
        paddingLeft: '165px',
        paddingTop: '12px',
        color: 'rgba(0, 0, 0, 0.45)',
    },
    tab: { fontSize: 20 },
};

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

    const onChangeSearchType = (newType: string) => {
        if (newType === ALL_ENTITIES_TAB_NAME) {
            navigateToSearchUrl({ query, page: 1, history, entityRegistry });
        } else {
            const entityType = entityRegistry.getTypeFromCollectionName(newType);
            navigateToSearchUrl({ type: entityType, query, page: 1, history, entityRegistry });
        }
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToSearchUrl({ type: activeType, query, page: 1, filters: newFilters, history, entityRegistry });
    };

    const onChangePage = (newPage: number) => {
        navigateToSearchUrl({ type: activeType, query, page: newPage, filters, history, entityRegistry });
    };

    return (
        <SearchablePage initialQuery={query} onSearch={onSearch}>
            <Affix offsetTop={80}>
                <Tabs
                    tabBarStyle={styles.tabs}
                    activeKey={activeType ? entityRegistry.getCollectionName(activeType) : ALL_ENTITIES_TAB_NAME}
                    size="large"
                    onChange={onChangeSearchType}
                >
                    <Tabs.TabPane
                        style={styles.tab}
                        tab={<span style={styles.tab}>All</span>}
                        key={ALL_ENTITIES_TAB_NAME}
                    />
                    {searchTypes.map((t) => (
                        <Tabs.TabPane
                            tab={
                                <>
                                    {entityRegistry.getIcon(t, 16, IconStyleType.TAB_VIEW)}{' '}
                                    <span style={styles.tab}>{entityRegistry.getCollectionName(t)}</span>
                                </>
                            }
                            key={entityRegistry.getCollectionName(t)}
                        />
                    ))}
                </Tabs>
            </Affix>
            {activeType ? (
                <EntitySearchResults
                    type={activeType}
                    page={page}
                    query={query}
                    filters={filters}
                    onChangeFilters={onChangeFilters}
                    onChangePage={onChangePage}
                />
            ) : (
                entityRegistry
                    .getSearchEntityTypes()
                    .map((entityType) => <EntityGroupSearchResults type={entityType} query={query} />)
            )}
        </SearchablePage>
    );
};
