import { Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { CLI_EXECUTOR_ID } from './utils';
import {
    LastStatusColumn,
    TypeColumn,
    ActionsColumn,
    ScheduleColumn,
    LastExecutionColumn,
} from './IngestionSourceTableColumns';
import { IngestionSource } from '../../../types.generated';
import { IngestionSourceExecutionList } from './executions/IngestionSourceExecutionList';

const StyledSourceTable = styled(StyledTable)`
    .cliIngestion {
        td {
            background-color: ${ANTD_GRAY[2]} !important;
        }
    }
` as typeof StyledTable;

interface Props {
    lastRefresh: number;
    sources: IngestionSource[];
    setFocusExecutionUrn: (urn: string) => void;
    onExecute: (urn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
    onRefresh: () => void;
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
}: Props) {
    const tableColumns = [
        {
            title: 'Type',
            dataIndex: 'type',
            key: 'type',
            render: TypeColumn,
            sorter: (sourceA, sourceB) => sourceA.type.localeCompare(sourceB.type),
        },
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name: string) => name || '',
            sorter: (sourceA, sourceB) => sourceA.name.localeCompare(sourceB.name),
        },
        {
            title: 'Schedule',
            dataIndex: 'schedule',
            key: 'schedule',
            render: ScheduleColumn,
        },
        {
            title: 'Execution Count',
            dataIndex: 'execCount',
            key: 'execCount',
            render: (execCount: any) => <Typography.Text>{execCount || '0'}</Typography.Text>,
            sorter: (sourceA, sourceB) => sourceA.execCount - sourceB.execCount,
        },
        {
            title: 'Last Execution',
            dataIndex: 'lastExecTime',
            key: 'lastExecTime',
            render: LastExecutionColumn,
            sorter: (sourceA, sourceB) => sourceA.lastExecTime - sourceB.lastExecTime,
        },
        {
            title: 'Last Status',
            dataIndex: 'lastExecStatus',
            key: 'lastExecStatus',
            render: (status: any, record) => (
                <LastStatusColumn status={status} record={record} setFocusExecutionUrn={setFocusExecutionUrn} />
            ),
            sorter: (sourceA, sourceB) => (sourceA.lastExecStatus || '').localeCompare(sourceB.lastExecStatus || ''),
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

    const tableData = sources.map((source) => ({
        urn: source.urn,
        type: source.type,
        name: source.name,
        platformUrn: source.platform?.urn,
        schedule: source.schedule?.interval,
        timezone: source.schedule?.timezone,
        execCount: source.executions?.total || 0,
        lastExecUrn:
            source.executions?.total && source.executions?.total > 0 && source.executions?.executionRequests[0].urn,
        lastExecTime:
            source.executions?.total &&
            source.executions?.total > 0 &&
            source.executions?.executionRequests[0].result?.startTimeMs,
        lastExecStatus:
            source.executions?.total &&
            source.executions?.total > 0 &&
            source.executions?.executionRequests[0].result?.status,
        cliIngestion: source.config.executorId === CLI_EXECUTOR_ID,
    }));

    return (
        <StyledSourceTable
            columns={tableColumns}
            dataSource={tableData}
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
