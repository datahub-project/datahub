import { Column, Table } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import { ActionsColumn } from '@app/ingestV2/executions/components/columns/ActionsColumn';
import { ExecutedByColumn } from '@app/ingestV2/executions/components/columns/ExecutedByColumn';
import SourceColumn from '@app/ingestV2/executions/components/columns/SourceColumn';
import { ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import DateTimeColumn from '@app/ingestV2/shared/components/columns/DateTimeColumn';
import DurationColumn from '@app/ingestV2/shared/components/columns/DurationColumn';
import { StatusColumn } from '@app/ingestV2/shared/components/columns/StatusColumn';
import { getIngestionSourceStatus } from '@app/ingestV2/source/utils';

import { ExecutionRequest } from '@types';

const StyledTable = styled(Table)`
    table-layout: fixed;
` as typeof Table;

interface Props {
    executionRequests: ExecutionRequest[];
    setFocusExecutionUrn: (urn: string) => void;
    loading?: boolean;
}

export default function ExecutionsTable({ executionRequests, setFocusExecutionUrn, loading }: Props) {
    const tableData: ExecutionRequestRecord[] = executionRequests.map((execution) => ({
        urn: execution.urn,
        name: execution?.source?.name,
        type: execution?.source?.type,
        actor: execution.input.actor,
        id: execution.id,
        source: execution.input.source.type,
        startedAt: execution.result?.startTimeMs,
        duration: execution.result?.durationMs,
        status: getIngestionSourceStatus(execution.result),
        // TODO:: getting this field form backend is not implemented yet
        showRollback: false,
        cliIngestion: execution.input.executorId === CLI_EXECUTOR_ID,
    }));

    const tableColumns: Column<ExecutionRequestRecord>[] = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => <SourceColumn record={record} />,
            width: '30%',
        },
        {
            title: 'Started At',
            key: 'startedAt',
            render: (record) => <DateTimeColumn time={record.startedAt} />,
            width: '15%',
        },
        {
            title: 'Duration',
            key: 'duration',
            render: (record) => <DurationColumn durationMs={record.duration} />,
            width: '15%',
        },
        {
            title: 'Executed By',
            key: 'executedBy',
            render: (record) => <ExecutedByColumn source={record.source} actor={record.actor} />,
            width: '30%',
        },
        {
            title: 'Status',
            key: 'status',
            render: (record) => (
                <StatusColumn status={record.status} onClick={() => setFocusExecutionUrn(record.urn)} />
            ),
            width: '15%',
        },
        {
            title: '',
            key: 'actions',
            render: (record) => <ActionsColumn record={record} setFocusExecutionUrn={setFocusExecutionUrn} />,
            width: '50px',
        },
    ];

    return <StyledTable columns={tableColumns} data={tableData} isScrollable isLoading={loading} />;
}
