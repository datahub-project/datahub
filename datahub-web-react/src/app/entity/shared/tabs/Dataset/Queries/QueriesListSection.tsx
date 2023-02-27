import React, { useEffect, useRef, useState } from 'react';
import { Pagination, Tooltip, Typography } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import QueriesList from './QueriesList';
import { Query } from './types';
import { DEFAULT_PAGE_SIZE } from './utils/constants';
import { getQueriesForPage } from './utils/getCurrentPage';
import { ANTD_GRAY } from '../../../constants';

const QueriesTitleSection = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 20px;
`;

const QueriesTitle = styled(Typography.Title)`
    && {
        margin: 0px;
    }
`;

const StyledPagination = styled(Pagination)`
    padding: 0px 24px 24px 24px;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    title: string;
    queries: Query[];
    tooltip?: string;
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
    tooltip,
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
        if (page !== 1) {
            (headerRef?.current as any)?.scrollIntoView({
                behavior: 'smooth',
                block: 'start',
                inline: 'nearest',
            });
        }
    }, [page]);

    return (
        <>
            <QueriesTitleSection>
                <QueriesTitle ref={headerRef} level={4}>
                    {title}
                </QueriesTitle>
                {tooltip && (
                    <Tooltip title={tooltip}>
                        <StyledInfoOutlined />
                    </Tooltip>
                )}
            </QueriesTitleSection>
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
