import React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Affix, Tabs } from 'antd';
import styled from 'styled-components';

import { SearchablePage } from './SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput } from '../../types.generated';
import useFilters from './utils/useFilters';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { EntitySearchResults } from './EntitySearchResults';
import { IconStyleType } from '../entity/Entity';
import { EntityGroupSearchResults } from './EntityGroupSearchResults';

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
                <StyledTabs
                    activeKey={activeType ? entityRegistry.getCollectionName(activeType) : ALL_ENTITIES_TAB_NAME}
                    size="large"
                    onChange={onChangeSearchType}
                >
                    <Tabs.TabPane tab={<StyledTab>All</StyledTab>} key={ALL_ENTITIES_TAB_NAME} />
                    {searchTypes.map((t) => (
                        <Tabs.TabPane
                            tab={
                                <>
                                    {entityRegistry.getIcon(t, 16, IconStyleType.TAB_VIEW)}
                                    <StyledTab>{entityRegistry.getCollectionName(t)}</StyledTab>
                                </>
                            }
                            key={entityRegistry.getCollectionName(t)}
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
                />
            ) : (
                entityRegistry
                    .getSearchEntityTypes()
                    .map((entityType) => <EntityGroupSearchResults type={entityType} query={query} />)
            )}
        </SearchablePage>
    );
};
