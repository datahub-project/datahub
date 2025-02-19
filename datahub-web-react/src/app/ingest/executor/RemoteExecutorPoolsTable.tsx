import { StyledTable } from '@src/app/entityV2/shared/components/styled/StyledTable';
import { RemoteExecutorPool } from '@src/types.generated';
import { Button, Empty, Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Check } from 'phosphor-react';
import { RemoteExecutorsList } from './RemoteExecutorsList';
import { PoolStatusColumn } from './Columns';

const PAGE_HEADER_HEIGHT = 395;
const ExecutorsTable = styled(StyledTable)`
    overflow: hidden;

    &&& .datahub-cloud-pool .ant-table-cell {
        background-color: #f9f9f9;
    }

    &&& .ant-table-body {
        overflow-y: auto;
        height: calc(100vh - ${PAGE_HEADER_HEIGHT}px);
    }
    &&& .ant-table-cell,
    &&& .ant-table-thead .ant-table-cell {
        background-color: #fff;
    }
` as typeof StyledTable;

const DefaultButton = styled(Button)`
    border: none;
    background: none;
    padding: 0;
    cursor: pointer;
    box-shadow: none;
    display: inline-block;
    &:hover {
        background: none;
    }
`;

type Props = {
    pools: RemoteExecutorPool[];
    onRefresh: () => void;
    updateDefaultPool: (urn: string) => void;
};

export const RemoteExecutorPoolsTable = ({ pools, onRefresh, updateDefaultPool }: Props) => {
    const tableData = pools.map((pool) => ({
        urn: pool.urn,
        isDataHubCloud: !!pool.remoteExecutors?.remoteExecutors?.find((executor) => executor.executorInternal),
        name: pool.poolName,
        reportedAt: Math.max(
            0,
            ...(pool.remoteExecutors?.remoteExecutors?.map((executor) => executor.reportedAt) ?? []),
        ),
        remoteExecutors: pool.remoteExecutors,
        isDefault: pool.isDefault,
    }));
    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name: string, record: (typeof tableData)[0]) => (
                <Typography.Text>
                    {name || 'unknown'}
                    {record.isDataHubCloud ? (
                        <strong style={{ opacity: 0.5, marginLeft: 4 }}> Hosted on DataHub Cloud</strong>
                    ) : (
                        ''
                    )}
                </Typography.Text>
            ),
        },
        {
            title: 'Executors Status',
            dataIndex: 'status',
            key: 'x',
            render: (_, record: (typeof tableData)[0]) => (
                <PoolStatusColumn executors={record.remoteExecutors?.remoteExecutors || []} />
            ),
        },
        {
            title: 'Last Reported',
            dataIndex: 'reportedAt',
            key: 'reportedAt',
            render: (reportedAt: number) =>
                reportedAt
                    ? `${new Date(reportedAt).toLocaleDateString()} at ${new Date(reportedAt).toLocaleTimeString()}`
                    : 'No status reported yet',
        },
        {
            title: 'Configuration',
            dataIndex: 'isDefault',
            key: 'isDefault',
            render: (isDefault: boolean, record: (typeof tableData)[0]) => (
                <Popover
                    content={
                        <Typography.Text>
                            {isDefault ? (
                                <>
                                    This pool will be selected by default
                                    <br />
                                    when new ingestion sources are created.
                                </>
                            ) : (
                                <>
                                    Make this pool the default selection
                                    <br />
                                    for new ingestion sources.
                                </>
                            )}
                        </Typography.Text>
                    }
                >
                    <DefaultButton onClick={() => updateDefaultPool(record.urn)}>
                        <Typography.Text type={isDefault ? undefined : 'secondary'} strong={isDefault}>
                            {isDefault ? (
                                <>
                                    <Check size={12} /> Recommended pool
                                </>
                            ) : (
                                'Recommend by default'
                            )}
                        </Typography.Text>
                    </DefaultButton>
                </Popover>
            ),
        },
    ];

    return (
        <ExecutorsTable
            columns={tableColumns}
            dataSource={tableData}
            scroll={{ y: 'max-content', x: 'max-content' }}
            rowKey="urn"
            rowClassName={(record) => (record.isDataHubCloud ? 'datahub-cloud-pool' : '')}
            locale={{
                // TODO: link to docs
                emptyText: (
                    <Empty description="No Remote Executors deployed yet." image={Empty.PRESENTED_IMAGE_SIMPLE} />
                ),
            }}
            expandable={{
                expandedRowRender: (data, _index, _indent, __) => {
                    return <RemoteExecutorsList remoteExecutors={data.remoteExecutors} onRefresh={onRefresh} />;
                },
                rowExpandable: (record) => {
                    return (record.remoteExecutors?.count ?? 0) > 0;
                },
                defaultExpandAllRows: false,
                indentSize: 0,
            }}
            pagination={false}
        />
    );
};
