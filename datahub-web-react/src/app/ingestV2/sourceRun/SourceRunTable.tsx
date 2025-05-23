import { Table } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import {
    ActionsColumn,
    LastExecutionColumn,
    NameColumn,
    ScheduleColumn,
    StatusColumn,
} from '@app/ingestV2/source/IngestionSourceTableColumns';
import { CLI_EXECUTOR_ID, getIngestionSourceStatus } from '@app/ingestV2/source/utils';

import { ExecutionRequest, IngestionSource } from '@types';
import DateTimeValue from '../shared/components/DateTimeValue';
import DurationValue from '../shared/components/DurationValue';

const StyledTable = styled(Table)`
    table-layout: fixed;
`;

interface Props {
    sources: ExecutionRequest[];
    setFocusExecutionUrn: (urn: string) => void;
    onExecute: (urn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
}

export default function SourceRunTable({ sources, setFocusExecutionUrn, onExecute, onEdit, onView, onDelete }: Props) {
    const tableData = sources.map((execution) => ({
        // TODO:: get this field from backend
        platform: undefined,
        // TODO:: get this field from backend
        name: 'Source name',
        type: execution.input.source.type,
        
        urn: execution.urn,
        actorUrn: execution.input.actorUrn,
        // TODO:: get this field from backend
        // actor: ...,
        id: execution.id,
        source: execution.input.source.type,
        requestedAt: execution.input.requestedAt,
        startedAt: execution.result?.startTimeMs,
        duration: execution.result?.durationMs,
        status: getIngestionSourceStatus(execution.result),
        // TODO:: get this field from backend?
        showRollback: false,
    }));

    const tableColumns = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return <NameColumn type={record.type} record={record} />;
            },
            width: '30%',
        },
        {
            title: 'Requested At',
            key: 'requestedAt',
            render: (record) => <DateTimeValue time={record.requestedAt ?? 0} />,
            width: '15%',
        },
        {
            title: 'Started At',
            key: 'startedAt',
            render: (record) => <DateTimeValue time={record.startedAt ?? 0} />,
            width: '15%',
        },
        {
            title: 'Duration',
            key: 'duration',
            render: (record) => <DurationValue durationMs={record.duration ?? 0}/>,
            width: '15%',
        },
        {
            title: 'Status',
            key: 'status',
            render: (record) => (
                <StatusColumn
                    status={record.lastExecStatus}
                    record={record}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                />
            ),
            width: '15%',
        },
        {
            title: 'Owner',
            key: 'owner',
            render: () => <></>,
            width: '15%',
        },
        {
            title: 'Executed By',
            key: 'executedBy',
            render: () => <></>,
            width: '15%',
        },
        {
            title: '',
            key: 'actions',
            render: (record) => (
                <ActionsColumn
                    record={record}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                    onExecute={onExecute}
                    onDelete={onDelete}
                    onView={onView}
                    onEdit={onEdit}
                />
            ),
            width: '100px',
        },
    ];

    return <StyledTable columns={tableColumns} data={tableData} isScrollable />;
}
