import { Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { SorterResult } from 'antd/lib/table/interface';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { CLI_EXECUTOR_ID, getIngestionSourceStatus } from './utils';
import {
    LastStatusColumn,
    TypeColumn,
    ActionsColumn,
    ScheduleColumn,
    LastExecutionColumn,
} from './IngestionSourceTableColumns';
import { IngestionSource } from '../../../types.generated';
import { IngestionSourceExecutionList } from './executions/IngestionSourceExecutionList';

const MIN_EXECUTION_COLUMN_WIDTH = 125;
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
}: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();

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
            title: 'Execution Count',
            dataIndex: 'execCount',
            key: 'execCount',
            width: isShowNavBarRedesign ? MIN_EXECUTION_COLUMN_WIDTH : undefined,
            render: (execCount: any) => <Typography.Text>{execCount || '0'}</Typography.Text>,
        },
        {
            title: 'Last Execution',
            dataIndex: 'lastExecTime',
            key: 'lastExecTime',
            render: LastExecutionColumn,
        },
        {
            title: 'Last Status',
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
    }));

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
