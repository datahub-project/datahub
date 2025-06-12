import { Column, Table } from '@components';
import { SorterResult } from 'antd/lib/table/interface';
import * as QueryString from 'query-string';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import TableFooter from '@app/ingestV2/shared/components/TableFooter';
import DateTimeColumn from '@app/ingestV2/shared/components/columns/DateTimeColumn';
import { StatusColumn } from '@app/ingestV2/shared/components/columns/StatusColumn';
import {
    ActionsColumn,
    NameColumn,
    OwnerColumn,
    ScheduleColumn,
} from '@app/ingestV2/source/IngestionSourceTableColumns';
import { IngestionSourceTableData } from '@app/ingestV2/source/types';
import { getIngestionSourceStatus } from '@app/ingestV2/source/utils';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import filtersToQueryStringParams from '@app/searchV2/utils/filtersToQueryStringParams';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { IngestionSource } from '@types';

const StyledTable = styled(Table)`
    table-layout: fixed;
` as typeof Table;

interface Props {
    sources: IngestionSource[];
    setFocusExecutionUrn: (urn: string) => void;
    onExecute: (urn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
    onChangeSort: (field: string, order: SorterResult<any>['order']) => void;
    isLoading?: boolean;
    shouldPreserveParams: React.MutableRefObject<boolean>;
    isLastPage?: boolean;
}

function IngestionSourceTable({
    sources,
    setFocusExecutionUrn,
    onExecute,
    onEdit,
    onView,
    onDelete,
    onChangeSort,
    isLoading,
    shouldPreserveParams,
    isLastPage,
}: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();

    const tableData: IngestionSourceTableData[] = sources.map((source) => ({
        urn: source.urn,
        type: source.type,
        name: source.name,
        platformUrn: source.platform?.urn,
        schedule: source.schedule?.interval,
        timezone: source.schedule?.timezone,
        execCount: source.executions?.total || 0,
        lastExecUrn: source.executions?.executionRequests?.[0]?.urn,
        lastExecTime: source.executions?.executionRequests?.[0]?.result?.startTimeMs,
        lastExecStatus:
            source.executions?.executionRequests?.[0]?.result &&
            getIngestionSourceStatus(source.executions.executionRequests[0].result),
        cliIngestion: source.config?.executorId === CLI_EXECUTOR_ID,
        owners: source.ownership?.owners,
    }));

    const tableColumns: Column<IngestionSourceTableData>[] = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return <NameColumn type={record.type} record={record} />;
            },
            width: '30%',
            sorter: true,
        },
        {
            title: 'Schedule',
            key: 'schedule',
            render: (record) => <ScheduleColumn schedule={record.schedule || ''} timezone={record.timezone || ''} />,
            width: '15%',
        },
        {
            title: 'Last Run',
            key: 'lastRun',
            render: (record) => <DateTimeColumn time={record.lastExecTime} />,
            width: '15%',
        },
        {
            title: 'Status',
            key: 'status',
            render: (record) => (
                <StatusColumn
                    status={record.lastExecStatus}
                    onClick={() => record.lastExecUrn && setFocusExecutionUrn(record.lastExecUrn)}
                    dataTestId="ingestion-source-table-status"
                />
            ),
            width: '15%',
        },
        {
            title: 'Owner',
            key: 'owner',
            render: (record) => <OwnerColumn owners={record.owners || []} entityRegistry={entityRegistry} />,
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

    const handleSortColumnChange = ({ sortColumn, sortOrder }) => {
        onChangeSort(sortColumn, sortOrder);
    };

    const onRowClick = (record) => {
        const selectedSourceNameFilter = [{ field: 'ingestionSource', values: [record.urn] }];
        const preserveParams = shouldPreserveParams;
        preserveParams.current = true;

        const search = QueryString.stringify(
            {
                ...filtersToQueryStringParams(selectedSourceNameFilter),
            },
            { arrayFormat: 'comma' },
        );

        history.replace({
            pathname: tabUrlMap[TabType.ExecutionLog],
            search,
        });
    };

    return (
        <StyledTable
            columns={tableColumns}
            data={tableData}
            isScrollable
            handleSortColumnChange={handleSortColumnChange}
            isLoading={isLoading}
            onRowClick={onRowClick}
            footer={
                isLastPage ? (
                    <TableFooter
                        hiddenItemsMessage="Some ingestion sources may be hidden"
                        colSpan={tableColumns.length}
                    />
                ) : null
            }
        />
    );
}
const MemoizedTable = React.memo(IngestionSourceTable);
export default MemoizedTable;
