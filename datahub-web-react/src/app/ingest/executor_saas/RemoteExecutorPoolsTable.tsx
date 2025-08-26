import { Button, Empty, Popover, Typography } from 'antd';
import { uniq } from 'lodash';
import { Check } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { PoolDescriptionColumn, PoolStatusColumn } from '@app/ingest/executor_saas/Columns';
import { RemoteExecutorsList } from '@app/ingest/executor_saas/RemoteExecutorsList';
import {
    checkIsPoolInDataHubCloud,
    getDisplayablePoolId,
    provisioningStatusToLabel,
} from '@app/ingest/executor_saas/utils';
import { checkIsExecutionRequestRunning } from '@app/ingest/source/utils';
import { colors } from '@src/alchemy-components';
import { StyledTable } from '@src/app/entityV2/shared/components/styled/StyledTable';
import { pluralize } from '@src/app/shared/textUtil';
import { RemoteExecutorPool, RemoteExecutorPoolStatus } from '@src/types.generated';

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

const LinkButton = styled(Button)`
    border: none;
    background: none;
    padding: 0;
    cursor: pointer;
    box-shadow: none;
    color: ${colors.blue[500]};
    display: inline-block;
    &:hover {
        background: none;
    }
`;

const ProvisioningStatusButton = styled(Button)`
    margin-left: 4px;
    color: ${colors.blue[500]};
    &:hover {
        background: none;
    }
`;

type Props = {
    pools: RemoteExecutorPool[];
    onRefresh: () => void;
    updateDefaultPool: (urn: string) => void;
    viewSourcesForPool: (executorPoolId: string) => void;
    viewPoolProvisioningStatus: (urn: string) => void;
};

export const RemoteExecutorPoolsTable = ({
    pools,
    onRefresh,
    updateDefaultPool,
    viewSourcesForPool,
    viewPoolProvisioningStatus,
}: Props) => {
    const tableData = pools.map((pool) => ({
        urn: pool.urn,
        isEmbedded: pool.isEmbedded,
        isDataHubCloud: checkIsPoolInDataHubCloud(pool),
        executorPoolId: pool.executorPoolId,
        description: pool.description,
        reportedAt: Math.max(
            0,
            ...(pool.remoteExecutors?.remoteExecutors?.map((executor) => executor.reportedAt) ?? []),
        ),
        remoteExecutors: pool.remoteExecutors,
        ingestionSources: pool.ingestionSources,
        isDefault: pool.isDefault,
        status: pool.state?.status,
    }));
    const tableColumns = [
        {
            title: 'Pool Identifier',
            dataIndex: 'executorPoolId',
            key: 'executorPoolId',
            render: (_, record: (typeof tableData)[0]) => (
                <Typography.Text>
                    {getDisplayablePoolId(record)}
                    {record.isDataHubCloud ? (
                        <strong style={{ opacity: 0.5, marginLeft: 4 }}> Hosted on DataHub Cloud</strong>
                    ) : (
                        ''
                    )}
                    {record.status !== RemoteExecutorPoolStatus.Ready && !record.isDataHubCloud ? (
                        <ProvisioningStatusButton type="text" onClick={() => viewPoolProvisioningStatus(record.urn)}>
                            {provisioningStatusToLabel(record.status)}
                        </ProvisioningStatusButton>
                    ) : null}
                </Typography.Text>
            ),
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (description: string, record: (typeof tableData)[0]) => (
                <PoolDescriptionColumn description={description} urn={record.urn} onUpdate={onRefresh} />
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
            title: 'Used by',
            dataIndex: 'x',
            key: 'ingestionSources',
            render: (_, record: (typeof tableData)[0]) => (
                <LinkButton onClick={() => viewSourcesForPool(record.executorPoolId)}>
                    {record.ingestionSources?.total ?? 0} {pluralize(record.ingestionSources?.total ?? 0, 'source')}
                </LinkButton>
            ),
        },
        {
            title: 'Tasks',
            dataIndex: 'x',
            key: 'runningTasks',
            render: (_, record: (typeof tableData)[0]) => (
                <Typography.Text>
                    {record.remoteExecutors?.remoteExecutors?.reduce(
                        (total, exec) =>
                            total +
                            (exec.recentExecutions?.executionRequests?.filter((task) =>
                                checkIsExecutionRequestRunning(task.result),
                            ).length || 0),
                        0,
                    ) || 'None'}
                </Typography.Text>
            ),
        },
        {
            title: 'Versions',
            dataIndex: 'x',
            key: 'versions',
            render: (_, record: (typeof tableData)[0]) => (
                <Typography.Text>
                    {uniq(record.remoteExecutors?.remoteExecutors?.map((exec) => exec.executorReleaseVersion))?.join(
                        ', ',
                    ) || 'Unknown'}
                </Typography.Text>
            ),
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
                                    <Check size={12} /> Default
                                </>
                            ) : (
                                'Make Default'
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
