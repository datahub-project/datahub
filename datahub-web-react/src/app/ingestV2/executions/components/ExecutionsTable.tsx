import { Column, Table } from '@components';
import * as QueryString from 'query-string';
import React, { useCallback, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import { ActionsColumn } from '@app/ingestV2/executions/components/columns/ActionsColumn';
import CancelExecutionConfirmation from '@app/ingestV2/executions/components/columns/CancelExecutionConfirmation';
import { ExecutedByColumn } from '@app/ingestV2/executions/components/columns/ExecutedByColumn';
import RollbackExecutionConfirmation from '@app/ingestV2/executions/components/columns/RollbackExecutionConfirmation';
import SourceColumn from '@app/ingestV2/executions/components/columns/SourceColumn';
import { ExecutionCancelInfo, ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import TableFooter from '@app/ingestV2/shared/components/TableFooter';
import DateTimeColumn from '@app/ingestV2/shared/components/columns/DateTimeColumn';
import DurationColumn from '@app/ingestV2/shared/components/columns/DurationColumn';
import { StatusColumn } from '@app/ingestV2/shared/components/columns/StatusColumn';
import { getIngestionSourceStatus } from '@app/ingestV2/source/utils';
import { TabType, tabUrlMap } from '@app/ingestV2/types';

import { ExecutionRequest } from '@types';

const StyledTable = styled(Table)`
    table-layout: fixed;
` as typeof Table;

interface Props {
    executionRequests: ExecutionRequest[];
    setFocusExecutionUrn: (urn: string) => void;
    loading?: boolean;
    handleRollback: (executionUrn: string) => void;
    handleCancelExecution: (executionUrn: string, ingestionSourceUrn: string) => void;
    isLastPage?: boolean;
}

export default function ExecutionsTable({
    executionRequests,
    setFocusExecutionUrn,
    loading,
    handleRollback,
    handleCancelExecution,
    isLastPage,
}: Props) {
    const [runIdOfRollbackConfirmation, setRunIdOfRollbackConfirmation] = useState<string | undefined>();
    const [executionInfoToCancel, setExecutionInfoToCancel] = useState<ExecutionCancelInfo | undefined>();
    const history = useHistory();

    const tableData: ExecutionRequestRecord[] = executionRequests.map((execution) => ({
        urn: execution.urn,
        name: execution?.source?.name,
        type: execution?.source?.type,
        actor: execution.input.actor,
        id: execution.id,
        source: execution.input.source.type,
        sourceUrn: execution.source?.urn,
        startedAt: execution.result?.startTimeMs,
        duration: execution.result?.durationMs,
        status: getIngestionSourceStatus(execution.result),
        showRollback: execution.source?.latestSuccessfulExecution?.urn === execution.urn,
        cliIngestion: execution.input.executorId === CLI_EXECUTOR_ID,
        privileges: execution.source?.privileges,
    }));

    const handleConfirmRollback = useCallback(() => {
        if (runIdOfRollbackConfirmation) handleRollback(runIdOfRollbackConfirmation);
        setRunIdOfRollbackConfirmation(undefined);
    }, [handleRollback, runIdOfRollbackConfirmation]);

    const navigateToSource = (record) => {
        history.replace({
            pathname: tabUrlMap[TabType.Sources],
            search: QueryString.stringify({ query: record.name }, { arrayFormat: 'comma' }),
        });
    };

    const handleConfirmCancel = useCallback(() => {
        if (executionInfoToCancel)
            handleCancelExecution(executionInfoToCancel.executionUrn, executionInfoToCancel.sourceUrn);
        setExecutionInfoToCancel(undefined);
    }, [handleCancelExecution, executionInfoToCancel]);

    const tableColumns: Column<ExecutionRequestRecord>[] = [
        {
            title: 'Source',
            key: 'source',
            render: (record) => <SourceColumn record={record} navigateToSource={() => navigateToSource(record)} />,
            width: '30%',
        },
        {
            title: 'Started At',
            key: 'startedAt',
            render: (record) => <DateTimeColumn time={record.startedAt} showRelative />,
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
            render: (record) => (
                <ActionsColumn
                    record={record}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                    handleRollback={() => setRunIdOfRollbackConfirmation(record.id)}
                    handleCancel={() => {
                        if (record.sourceUrn) {
                            setExecutionInfoToCancel({ executionUrn: record.urn, sourceUrn: record.sourceUrn });
                        } else {
                            console.error(`The ingestion source is undefined for execution ${record.id}`);
                        }
                    }}
                />
            ),
            width: '100px',
        },
    ];

    return (
        <>
            <StyledTable
                columns={tableColumns}
                data={tableData}
                isScrollable
                isLoading={loading}
                onRowClick={(record) => setFocusExecutionUrn(record.urn)}
                footer={
                    isLastPage ? (
                        <TableFooter hiddenItemsMessage="Some executions may be hidden" colSpan={tableColumns.length} />
                    ) : null
                }
            />
            <RollbackExecutionConfirmation
                isOpen={!!runIdOfRollbackConfirmation}
                onConfirm={() => handleConfirmRollback()}
                onCancel={() => setRunIdOfRollbackConfirmation(undefined)}
            />
            <CancelExecutionConfirmation
                isOpen={!!executionInfoToCancel}
                onConfirm={() => handleConfirmCancel()}
                onCancel={() => setExecutionInfoToCancel(undefined)}
            />
        </>
    );
}
