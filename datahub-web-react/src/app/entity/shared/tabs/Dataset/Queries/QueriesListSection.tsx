import React, { useEffect, useRef, useState } from 'react';
import { Pagination, Typography } from 'antd';
import styled from 'styled-components';
import QueriesList from './QueriesList';
import { Query } from './types';
import { DEFAULT_PAGE_SIZE } from './utils/constants';
import { getQueriesForPage } from './utils/getCurrentPage';

const QueriesTitle = styled(Typography.Title)`
    && {
        margin-bottom: 20px;
    }
`;

const StyledPagination = styled(Pagination)`
    padding: 0px 24px 24px 24px;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
`;

type Props = {
    title: string;
    queries: Query[];
    initialPage?: number;
    initialPageSize?: number;
    showDetails?: boolean;
    showEdit?: boolean;
    showDelete?: boolean;
    onDeleted?: (query) => void;
    onEdited?: (query) => void;
};

export default function QueriesListSection({
    title,
    queries,
    initialPage = 1,
    initialPageSize = DEFAULT_PAGE_SIZE,
    showDetails,
    showEdit,
    showDelete,
    onDeleted,
    onEdited,
}: Props) {
    /**
     * Pagination State
     */
    const [page, setPage] = useState(initialPage);
    const [pageSize, setPageSize] = useState(initialPageSize);
    const paginatedQueries = getQueriesForPage(queries, page, pageSize);

    /**
     * For scrolling to top of section on page change.
     */
    const headerRef = useRef(null);
    useEffect(() => {
        (headerRef?.current as any)?.scrollIntoView({
            behavior: 'smooth',
            block: 'start',
            inline: 'nearest',
        });
    }, [page]);

    return (
        <>
            <QueriesTitle ref={headerRef} level={4}>
                {title}
            </QueriesTitle>
            <QueriesList
                queries={paginatedQueries}
                showDelete={showDelete}
                showEdit={showEdit}
                showDetails={showDetails}
                onDeleted={onDeleted}
                onEdited={onEdited}
            />
            {queries.length > pageSize && (
                <StyledPagination
                    current={page}
                    pageSize={pageSize}
                    total={queries.length}
                    showLessItems
                    onChange={(newPage) => {
                        setPage(newPage);
                    }}
                    onShowSizeChange={(_currSize, newSize) => setPageSize(newSize)}
                    showSizeChanger={false}
                />
            )}
        </>
    );
}
