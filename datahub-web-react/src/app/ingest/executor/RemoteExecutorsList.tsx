import { StyledTable } from '@src/app/entityV2/shared/components/styled/StyledTable';
import { ListRemoteExecutorsResult, Maybe } from '@src/types.generated';
import { Button, Empty, Pagination, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ArrowClockwise } from 'phosphor-react';
import { DetailsColumn, StatusColumn, TimeColumn } from './Columns';

const PaginationInfoContainer = styled.span`
    padding: 8px;
    padding-left: 16px;
    border-top: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 0px;
    padding: 0px;
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
`;

const Table = styled(StyledTable)`` as typeof StyledTable;

type Props = {
    remoteExecutors?: Maybe<ListRemoteExecutorsResult>;
    onRefresh: () => void;
};

export const RemoteExecutorsList = ({ remoteExecutors, onRefresh }: Props) => {
    const [page, setPage] = useState(1);
    // TODO: paginate - for now we just fetch all the executors
    // const numResultsPerPage = 10000;
    // const start: number = (page - 1) * numResultsPerPage;
    const totalExecutors = remoteExecutors?.total || 0;
    const pageSize = remoteExecutors?.count || 0;
    const pageStart = remoteExecutors?.start || 0;
    const lastResultIndex = pageStart + pageSize > totalExecutors ? totalExecutors : pageStart + pageSize;

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const tableData = remoteExecutors?.remoteExecutors || [];

    const tableColumns = [
        {
            title: 'ID',
            dataIndex: 'urn',
            key: 'urn',
            render: (urn: string) => urn?.replace('urn:li:dataHubRemoteExecutor:', '') || 'Unknown',
        },
        {
            title: 'Version',
            dataIndex: 'executorReleaseVersion',
            key: 'executorReleaseVersion',
            render: (version: string) => version || 'Unknown',
        },
        {
            title: 'Last reported',
            dataIndex: 'reportedAt',
            key: 'reportedAt',
            render: TimeColumn,
        },
        {
            title: 'Status',
            dataIndex: 'x',
            key: 'x',
            render: (_, record) => <StatusColumn {...record} />,
        },
        {
            title: 'Details',
            dataIndex: 'x',
            key: 'x',
            render: (_, record) => <DetailsColumn executor={record} />,
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: () => (
                <Button onClick={onRefresh} style={{ border: 0, boxShadow: 'none', backgroundColor: 'transparent' }}>
                    <ArrowClockwise />
                </Button>
            ),
        },
    ];

    return (
        <>
            <Table
                columns={tableColumns}
                dataSource={tableData}
                rowKey="id"
                locale={{
                    emptyText: <Empty description="No Executors found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                pagination={false}
            />
            <PaginationInfoContainer>
                <PaginationInfo>
                    <b>
                        {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                    </b>{' '}
                    of <b>{totalExecutors}</b>
                </PaginationInfo>
                <StyledPagination
                    current={page}
                    pageSize={pageSize}
                    total={totalExecutors}
                    // showLessItems
                    onChange={onChangePage}
                />
            </PaginationInfoContainer>
        </>
    );
};
