import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useLocation } from 'react-router';
import { Button, message, Pagination } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import * as QueryString from 'query-string';
import { useListMyViewsQuery } from '../../../graphql/view.generated';
import { SearchBar } from '../../search/SearchBar';
import TabToolbar from '../shared/components/styled/TabToolbar';
import { Message } from '../../shared/Message';
import { useEntityRegistry } from '../../useEntityRegistry';
import { scrollToTop } from '../../shared/searchUtils';
import { ViewsTable } from './ViewsTable';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE, searchViews } from './utils';
import { ViewBuilder } from './builder/ViewBuilder';
import { ViewBuilderMode } from './builder/types';

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

const searchBarStyle = {
    maxWidth: 220,
    padding: 0,
};

const searchBarInputStyle = {
    height: 32,
    fontSize: 12,
};

/**
 * This component renders a paginated, searchable list of Views.
 */
export const ViewsList = () => {
    /**
     * Context
     */
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    /**
     * Query Params
     */
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;

    /**
     * State
     */
    const [page, setPage] = useState(1);
    const [selectedViewUrn, setSelectedViewUrn] = useState<undefined | string>(undefined);
    const [showViewBuilder, setShowViewBuilder] = useState<boolean>(false);
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    /**
     * Queries
     */
    const pageSize = DEFAULT_LIST_VIEWS_PAGE_SIZE;
    const start = (page - 1) * pageSize;
    const { loading, error, data } = useListMyViewsQuery({
        variables: {
            start,
            count: pageSize,
        },
        fetchPolicy: 'cache-first',
    });

    const onClickCreateView = () => {
        setShowViewBuilder(true);
    };

    const onClickEditView = (urn: string) => {
        setShowViewBuilder(true);
        setSelectedViewUrn(urn);
    };

    const onCloseModal = () => {
        setShowViewBuilder(false);
        setSelectedViewUrn(undefined);
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    /**
     * Render variables.
     */
    const totalViews = data?.listMyViews?.total || 0;
    const views = searchViews(data?.listMyViews?.views || [], query);
    const selectedView = (selectedViewUrn && views.find((view) => view.urn === selectedViewUrn)) || undefined;

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading Views..." />}
            {error && message.error({ content: `Failed to load Views! An unexpected error occurred.`, duration: 3 })}
            <TabToolbar>
                <Button type="text" onClick={onClickCreateView}>
                    <PlusOutlined /> Create new View
                </Button>
                <SearchBar
                    initialQuery=""
                    placeholderText="Search Views..."
                    suggestions={[]}
                    style={searchBarStyle}
                    inputStyle={searchBarInputStyle}
                    onSearch={() => null}
                    onQueryChange={(q) => setQuery(q.length > 0 ? q : undefined)}
                    entityRegistry={entityRegistry}
                />
            </TabToolbar>
            <ViewsTable views={views} onEditView={onClickEditView} />
            {totalViews >= pageSize && (
                <PaginationContainer>
                    <StyledPagination
                        current={page}
                        pageSize={pageSize}
                        total={totalViews}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </PaginationContainer>
            )}
            {showViewBuilder && (
                <ViewBuilder
                    mode={ViewBuilderMode.EDITOR}
                    urn={selectedViewUrn}
                    initialState={selectedView}
                    onSubmit={onCloseModal}
                    onCancel={onCloseModal}
                />
            )}
        </>
    );
};
