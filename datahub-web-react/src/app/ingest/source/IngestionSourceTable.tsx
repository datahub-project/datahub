<<<<<<< HEAD
import { Button, Empty } from 'antd';
||||||| 952f3cc3118
import { Empty, Typography } from 'antd';
=======
import { Empty } from 'antd';
>>>>>>> master
import React from 'react';
import styled from 'styled-components/macro';
import { SorterResult } from 'antd/lib/table/interface';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { colors } from '@src/alchemy-components';
import { useAppConfig } from '@src/app/useAppConfig';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { CLI_EXECUTOR_ID, getIngestionSourceStatus } from './utils';
import { LastStatusColumn, TypeColumn, ActionsColumn, ScheduleColumn } from './IngestionSourceTableColumns';
import { IngestionSource } from '../../../types.generated';
import { IngestionSourceExecutionList } from './executions/IngestionSourceExecutionList';
import { getDisplayablePoolId } from '../executor_saas/utils';

const PAGE_HEADER_HEIGHT = 395;

const StyledSourceTable = styled(StyledTable)`
    .cliIngestion {
        td {
            background-color: ${ANTD_GRAY[2]} !important;
        }
    }
` as typeof StyledTable;

const StyledSourceTableWithNavBarRedesign = styled(StyledSourceTable)`
    overflow: hidden;

    &&& .ant-table-body {
        overflow-y: auto;
        height: calc(100vh - ${PAGE_HEADER_HEIGHT}px);
    }
` as typeof StyledSourceTable;

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

interface Props {
    lastRefresh: number;
    sources: IngestionSource[];
    setFocusExecutionUrn: (urn: string) => void;
    onExecute: (urn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
    onRefresh: () => void;
    onChangeSort: (field: string, order: SorterResult<any>['order']) => void;
    saasProps: {
        onViewPool: (poolId: string) => void;
    };
}

function IngestionSourceTable({
    lastRefresh,
    sources,
    setFocusExecutionUrn,
    onExecute,
    onEdit,
    onView,
    onDelete,
    onRefresh,
    onChangeSort,
    saasProps,
}: Props) {
    const appConfig = useAppConfig();
    const isPoolsDisplayEnabled = appConfig.config.featureFlags.displayExecutorPools;
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const tableData = sources.map((source) => ({
        urn: source.urn,
        type: source.type,
        name: source.name,
        platformUrn: source.platform?.urn,
        schedule: source.schedule?.interval,
        timezone: source.schedule?.timezone,
        execCount: source.executions?.total || 0,
        lastExecUrn:
            source.executions &&
            source.executions?.executionRequests?.length > 0 &&
            source.executions?.executionRequests[0]?.urn,
        lastExecTime:
            source.executions &&
            source.executions?.executionRequests?.length > 0 &&
            source.executions?.executionRequests[0]?.result?.startTimeMs,
        lastExecStatus:
            source.executions &&
            source.executions?.executionRequests?.length > 0 &&
            getIngestionSourceStatus(source.executions?.executionRequests[0]?.result),
        cliIngestion: source.config?.executorId === CLI_EXECUTOR_ID,
        executorPoolId: source.config.executorId, // SaaS only
    }));

<<<<<<< HEAD
    const tableColumns = [
        {
            title: 'Type',
            dataIndex: 'type',
            key: 'type',
            render: (type: string, record: any) => <TypeColumn type={type} record={record} />,
            sorter: true,
        },
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name: string) => name || '',
            sorter: true,
        },
        {
            title: 'Schedule',
            dataIndex: 'schedule',
            key: 'schedule',
            render: ScheduleColumn,
        },
        ...(isPoolsDisplayEnabled
            ? [
                  {
                      // SaaS only
                      title: 'Executor Pool',
                      dataIndex: 'x',
                      key: 'x',
                      render: (_, record: (typeof tableData)[0]) =>
                          record.executorPoolId && !record.cliIngestion ? (
                              <LinkButton onClick={() => saasProps.onViewPool(record.executorPoolId)}>
                                  {getDisplayablePoolId({ executorPoolId: record.executorPoolId })}
                              </LinkButton>
                          ) : (
                              <span>{record.cliIngestion ? 'N/A' : 'Unknown'}</span>
                          ),
                  },
              ]
            : []),
        {
            title: 'Status',
            dataIndex: 'lastExecStatus',
            key: 'lastExecStatus',
            render: (status: any, record) => (
                <LastStatusColumn status={status} record={record} setFocusExecutionUrn={setFocusExecutionUrn} />
            ),
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionsColumn
                    record={record}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                    onExecute={onExecute}
                    onDelete={onDelete}
                    onView={onView}
                    onEdit={onEdit}
                />
            ),
        },
    ];

||||||| 952f3cc3118
=======
    const tableColumns = [
        {
            title: 'Type',
            dataIndex: 'type',
            key: 'type',
            render: (type: string, record: any) => <TypeColumn type={type} record={record} />,
            sorter: true,
        },
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name: string) => name || '',
            sorter: true,
        },
        {
            title: 'Schedule',
            dataIndex: 'schedule',
            key: 'schedule',
            render: ScheduleColumn,
        },
        {
            title: 'Status',
            dataIndex: 'lastExecStatus',
            key: 'lastExecStatus',
            render: (status: any, record) => (
                <LastStatusColumn status={status} record={record} setFocusExecutionUrn={setFocusExecutionUrn} />
            ),
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionsColumn
                    record={record}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                    onExecute={onExecute}
                    onDelete={onDelete}
                    onView={onView}
                    onEdit={onEdit}
                />
            ),
        },
    ];

>>>>>>> master
    const handleTableChange = (_: any, __: any, sorter: any) => {
        const sorterTyped: SorterResult<any> = sorter;
        const field = sorterTyped.field as string;
        const { order } = sorterTyped;
        onChangeSort(field, order);
    };

    const FinalStyledSourceTable = isShowNavBarRedesign ? StyledSourceTableWithNavBarRedesign : StyledSourceTable;

    return (
        <FinalStyledSourceTable
            columns={tableColumns}
            onChange={handleTableChange}
            dataSource={tableData}
            scroll={isShowNavBarRedesign ? { y: 'max-content', x: 'max-content' } : {}}
            rowKey="urn"
            rowClassName={(record, _) => (record.cliIngestion ? 'cliIngestion' : '')}
            locale={{
                emptyText: <Empty description="No Ingestion Sources!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            expandable={{
                expandedRowRender: (record, _index, _indent, expanded) => {
                    return (
                        <IngestionSourceExecutionList
                            urn={record.urn}
                            isExpanded={expanded}
                            lastRefresh={lastRefresh}
                            onRefresh={onRefresh}
                            saasProps={{ onViewPool: saasProps.onViewPool }}
                        />
                    );
                },
                rowExpandable: (record) => {
                    return record.execCount > 0;
                },
                defaultExpandAllRows: false,
                indentSize: 0,
            }}
            pagination={false}
        />
    );
}

export default IngestionSourceTable;
