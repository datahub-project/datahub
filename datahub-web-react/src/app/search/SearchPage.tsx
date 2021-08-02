import React, { useMemo, useEffect } from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Affix, Tabs } from 'antd';
import styled from 'styled-components';

import { SearchablePage } from './SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput, EntityType } from '../../types.generated';
import useFilters from './utils/useFilters';
import { useGetAllEntitySearchResults } from '../../utils/customGraphQL/useGetAllEntitySearchResults';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { countFormatter } from '../../utils/formatter';
import { EntitySearchResults } from './EntitySearchResults';
import { IconStyleType } from '../entity/Entity';
import { AllEntitiesSearchResults } from './AllEntitiesSearchResults';
import analytics, { EventType } from '../analytics';

const ALL_ENTITIES_TAB_NAME = 'All';

const StyledTabs = styled(Tabs)`
     {
        background-color: ${(props) => props.theme.styles['body-background']};
        .ant-tabs-nav {
            padding-left: 165px;
            margin-bottom: 0px;
        }
        padding-top: 12px;
        margin-bottom: 16px;
    }
`;

const StyledTab = styled.span`
    &&& {
        font-size: 20px;
    }
`;
const StyledNumberInTab = styled.span`
    &&& {
        padding-left: 8px;
        font-size: 20px;
        color: gray;
    }
`;

type SearchPageParams = {
    type?: string;
};

type SearchResultCounts = {
    [key in EntityType]?: number;
};

const RESULTS_PER_GROUP = 3; // Results limit per entities

/**
 * A search results page.
 */
export const SearchPage = () => {
    const history = useHistory();
    const location = useLocation();

    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = params.query ? (params.query as string) : '';
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);

    const allSearchResultsByType = useGetAllEntitySearchResults({
        query,
        start: 0,
        count: RESULTS_PER_GROUP,
        filters: null,
    });

    const loading = Object.keys(allSearchResultsByType).some((type) => {
        return allSearchResultsByType[type].loading;
    });

    const noResults = Object.keys(allSearchResultsByType).every((type) => {
        return (
            !allSearchResultsByType[type].loading &&
            allSearchResultsByType[type].data?.search?.searchResults.length === 0
        );
    });

    const resultCounts: SearchResultCounts = useMemo(() => {
        if (!loading) {
            const counts: SearchResultCounts = {};
            Object.keys(allSearchResultsByType).forEach((key) => {
                if (!allSearchResultsByType[key].loading) {
                    counts[key as EntityType] = allSearchResultsByType[key].data?.search?.total || 0;
                }
            });
            return counts;
        }
        return {};
    }, [allSearchResultsByType, loading]);

    useEffect(() => {
        if (!loading) {
            let resultCount = 0;
            Object.keys(allSearchResultsByType).forEach((key) => {
                if (!allSearchResultsByType[key].loading) {
                    resultCount += allSearchResultsByType[key].data?.search?.total;
                }
            });

            analytics.event({
                type: EventType.SearchResultsViewEvent,
                query,
                total: resultCount,
            });
        }
    }, [query, allSearchResultsByType, loading]);

    const onSearch = (q: string, type?: EntityType) => {
        if (q.trim().length === 0) {
            return;
        }
        analytics.event({
            type: EventType.SearchEvent,
            query: q,
            entityTypeFilter: activeType,
            pageNumber: 1,
            originPath: window.location.pathname,
        });
        navigateToSearchUrl({ type: type || activeType, query: q, page: 1, history, entityRegistry });
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

    const filteredSearchTypes =
        resultCounts && Object.keys(resultCounts).length > 0
            ? searchTypes.filter((type: EntityType) => !!resultCounts[type] && (resultCounts[type] as number) > 0)
            : [];

    return (
        <SearchablePage initialQuery={query} onSearch={onSearch}>
            <Affix offsetTop={80}>
                <StyledTabs
                    activeKey={activeType ? entityRegistry.getCollectionName(activeType) : ALL_ENTITIES_TAB_NAME}
                    size="large"
                    onChange={onChangeSearchType}
                >
                    <Tabs.TabPane tab={<StyledTab>All</StyledTab>} key={ALL_ENTITIES_TAB_NAME} />
                    {filteredSearchTypes.map((type: EntityType) => (
                        <Tabs.TabPane
                            tab={
                                <>
                                    {entityRegistry.getIcon(type, 16, IconStyleType.TAB_VIEW)}
                                    <StyledTab>{entityRegistry.getCollectionName(type)}</StyledTab>
                                    {resultCounts[type] ? (
                                        <StyledNumberInTab>{`${countFormatter(
                                            resultCounts[type] || 0,
                                        )}`}</StyledNumberInTab>
                                    ) : null}
                                </>
                            }
                            key={entityRegistry.getCollectionName(type)}
                        />
                    ))}
                </StyledTabs>
            </Affix>
            {activeType ? (
                <EntitySearchResults
                    type={activeType}
                    page={page}
                    query={query}
                    filters={filters}
                    onChangeFilters={onChangeFilters}
                    onChangePage={onChangePage}
                    searchResult={allSearchResultsByType[activeType]}
                />
            ) : (
                <AllEntitiesSearchResults
                    query={query}
                    allSearchResultsByType={allSearchResultsByType}
                    loading={loading}
                    noResults={noResults}
                />
            )}
        </SearchablePage>
    );
};
