import { SearchBar, Text } from '@components';
import { Pagination, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ViewsTable } from '@app/entityV2/view/ViewsTable';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE, searchViews } from '@app/entityV2/view/utils';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';

import {
    ListGlobalViewsQuery,
    ListMyViewsQuery,
    useListGlobalViewsQuery,
    useListMyViewsQuery,
} from '@graphql/view.generated';
import { DataHubViewType } from '@types';

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

type Props = {
    viewType?: DataHubViewType;
};

const StyledTabToolbar = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 1px 0 16px 0; // 1px at the top to prevent Select's border outline from cutting-off
    height: auto;
    z-index: unset;
    box-shadow: none;
    flex-shrink: 0;
`;

export const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
    gap: 16px;

    svg {
        width: 160px;
        height: 160px;
    }
`;

const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    max-height: calc(100vh - 330px); /* Constrain to page height minus header/filters space */
    overflow: auto;

    /* Make table header sticky */
    .ant-table-thead {
        position: sticky;
        top: 0;
        z-index: 1;
        background: white;
    }

    /* Ensure header cells have proper background */
    .ant-table-thead > tr > th {
        background: white !important;
        border-bottom: 1px solid #f0f0f0;
    }
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-top: 8px;
`;

const ViewsContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    padding-top: 7px;
`;

const StyledSearchBar = styled(SearchBar)`
    width: 300px;
`;

/**
 * This component renders a paginated, searchable list of Views.
 */
export const ViewsList = ({ viewType = DataHubViewType.Personal }: Props) => {
    /**
     * State
     */
    const [page, setPage] = useState(1);
    const [query, setQuery] = useState<undefined | string>(undefined);

    /**
     * Queries
     */
    const pageSize = DEFAULT_LIST_VIEWS_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const isPersonal = viewType === DataHubViewType.Personal;
    const viewsQuery = isPersonal ? useListMyViewsQuery : useListGlobalViewsQuery;

    const { loading, error, data } = viewsQuery({
        variables: {
            start,
            count: pageSize,
        },
        fetchPolicy: 'cache-first',
    });

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    /**
     * Render variables.
     */
    const viewsData = isPersonal
        ? (data as ListMyViewsQuery)?.listMyViews
        : (data as ListGlobalViewsQuery)?.listGlobalViews;
    const totalViews = viewsData?.total || 0;
    const views = searchViews(viewsData?.views || [], query);

    if (!totalViews) {
        return (
            <EmptyContainer>
                <Text size="md" color="gray" weight="bold">
                    No Views yet!
                </Text>
            </EmptyContainer>
        );
    }

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading Views..." />}
            {error && message.error({ content: `Failed to load Views! An unexpected error occurred.`, duration: 3 })}
            <ViewsContainer>
                <StyledTabToolbar>
                    <SearchContainer>
                        <StyledSearchBar placeholder="Search Views..." onChange={setQuery} value={query || ''} />
                    </SearchContainer>
                </StyledTabToolbar>
                <TableContainer>
                    <ViewsTable views={views} />
                </TableContainer>
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
            </ViewsContainer>
        </>
    );
};
