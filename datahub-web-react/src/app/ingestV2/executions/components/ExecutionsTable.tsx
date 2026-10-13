import { Column, Table } from '@components';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
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
import DateTimeColumn, { wrapDateTimeColumnWithHover } from '@app/ingestV2/shared/components/columns/DateTimeColumn';
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
    handleRollback: (executionUrn: string) => void;
    handleCancelExecution: (executionUrn: string, ingestionSourceUrn: string) => void;
    isLastPage?: boolean;
    /** Invoked when the user clicks a source name in a run row. Owns navigation, since the host
     * page decides what "go to this source" means. */
    onSourceClick: (record: ExecutionRequestRecord) => void;
    /** Returns the route to a run-details page for the given execution urn, or undefined to fall
     * back to the legacy in-place modal (setFocusExecutionUrn). */
    getRunDetailsPath: (urn: string) => string | undefined;
    /** Stashed as location state on the run-details route so its breadcrumb can link back here. */
    runDetailsFromUrl: string;
}

export default function ExecutionsTable({
    executionRequests,
    setFocusExecutionUrn,
    loading,
    handleRollback,
    handleCancelExecution,
    isLastPage,
    onSourceClick,
    getRunDetailsPath,
    runDetailsFromUrl,
}: Props) {
    const { t } = useTranslation('ingestion');
    const { t: tl } = useTranslation('common.labels');
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
    }));

    const handleConfirmRollback = useCallback(() => {
        if (runIdOfRollbackConfirmation) handleRollback(runIdOfRollbackConfirmation);
        setRunIdOfRollbackConfirmation(undefined);
    }, [handleRollback, runIdOfRollbackConfirmation]);

    const handleConfirmCancel = useCallback(() => {
        if (executionInfoToCancel)
            handleCancelExecution(executionInfoToCancel.executionUrn, executionInfoToCancel.sourceUrn);
        setExecutionInfoToCancel(undefined);
    }, [handleCancelExecution, executionInfoToCancel]);

    const handleViewDetails = useCallback(
        (urn: string) => {
            const path = getRunDetailsPath(urn);
            if (path) {
                history.push(path, { fromUrl: runDetailsFromUrl });
            } else {
                setFocusExecutionUrn(urn);
            }
        },
        [history, setFocusExecutionUrn, getRunDetailsPath, runDetailsFromUrl],
    );

    const tableColumns: Column<ExecutionRequestRecord>[] = [
        {
            title: tl('source'),
            key: 'source',
            render: (record) => <SourceColumn record={record} navigateToSource={() => onSourceClick(record)} />,
            width: '30%',
        },
        {
            title: t('executions.colStartedAt'),
            key: 'startedAt',
            render: (record) => <DateTimeColumn time={record.startedAt} showRelative />,
            width: '15%',
            cellWrapper: (content, record) => wrapDateTimeColumnWithHover(content, record.startedAt),
        },
        {
            title: t('executions.colDuration'),
            key: 'duration',
            render: (record) => <DurationColumn durationMs={record.duration} />,
            width: '15%',
        },
        {
            title: t('executions.colExecutedBy'),
            key: 'executedBy',
            render: (record) => <ExecutedByColumn source={record.source} actor={record.actor} />,
            width: '30%',
        },
        {
            title: tl('status'),
            key: 'status',
            render: (record) => <StatusColumn status={record.status} onClick={() => handleViewDetails(record.urn)} />,
            width: '15%',
        },
        {
            title: '',
            key: 'actions',
            render: (record) => (
                <ActionsColumn
                    record={record}
                    handleViewDetails={handleViewDetails}
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
                onRowClick={(record) => handleViewDetails(record.urn)}
                rowDataTestId={(record) => `execution-row-${record.urn}`}
                footer={
                    isLastPage ? (
                        <TableFooter hiddenItemsMessage={t('executions.someHidden')} colSpan={tableColumns.length} />
                    ) : null
                }
                data-testid="executions-table"
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
