import { Column, Table } from '@components';
import { SorterResult } from 'antd/lib/table/interface';
import * as QueryString from 'query-string';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import TableFooter from '@app/ingestV2/shared/components/TableFooter';
import DateTimeColumn, { wrapDateTimeColumnWithHover } from '@app/ingestV2/shared/components/columns/DateTimeColumn';
import { StatusColumn } from '@app/ingestV2/shared/components/columns/StatusColumn';
import {
    ActionsColumn,
    NameColumn,
    OwnerColumn,
    ScheduleColumn,
    wrapOwnerColumnWithHover,
} from '@app/ingestV2/source/IngestionSourceTableColumns';
import { IngestionSourceTableData } from '@app/ingestV2/source/types';
import { getSourceStatus } from '@app/ingestV2/source/utils';
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
    onCancelExecution: (executionUrn: string | undefined, ingestionSourceUrn: string) => void;
    onEdit: (urn: string) => void;
    onView: (urn: string) => void;
    onDelete: (urn: string) => void;
    onChangeSort: (field: string, order: SorterResult<any>['order']) => void;
    isLoading?: boolean;
    shouldPreserveParams: React.MutableRefObject<boolean>;
    isLastPage?: boolean;
    sourcesToRefetch: Set<string>;
    executedUrns: Set<string>;
    setSelectedTab: (selectedTab: TabType | null | undefined) => void;
}

function IngestionSourceTable({
    sources,
    setFocusExecutionUrn,
    onExecute,
    onCancelExecution,
    onEdit,
    onView,
    onDelete,
    onChangeSort,
    isLoading,
    shouldPreserveParams,
    isLastPage,
    sourcesToRefetch,
    executedUrns,
    setSelectedTab,
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
        lastExecStatus: getSourceStatus(source, sourcesToRefetch, executedUrns),
        cliIngestion: source.config?.executorId === CLI_EXECUTOR_ID,
        owners: source.ownership?.owners,
    }));

    const navigateToRunHistory = (record) => {
        setSelectedTab(TabType.RunHistory);
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
            pathname: tabUrlMap[TabType.RunHistory],
            search,
        });
    };

    const tableColumns: Column<IngestionSourceTableData>[] = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return <NameColumn type={record.type} record={record} />;
            },
            width: '25%',
            sorter: true,
            onCellClick: (record) => onEdit(record.urn),
        },
        {
            title: 'Schedule',
            key: 'schedule',
            render: (record) => <ScheduleColumn schedule={record.schedule || ''} timezone={record.timezone || ''} />,
            width: '20%',
        },
        {
            title: 'Owner',
            key: 'owner',
            render: (record) => <OwnerColumn owners={record.owners || []} entityRegistry={entityRegistry} />,
            width: '20%',
            cellWrapper: wrapOwnerColumnWithHover,
        },
        {
            title: 'Last Run',
            key: 'lastRun',
            render: (record) => <DateTimeColumn time={record.lastExecTime} showRelative />,
            width: '20%',
            onCellClick: (record) => navigateToRunHistory(record),
            cellWrapper: wrapDateTimeColumnWithHover,
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
            onCellClick: (record) => record.lastExecUrn && setFocusExecutionUrn(record.lastExecUrn),
        },

        {
            title: '',
            key: 'actions',
            render: (record) => (
                <ActionsColumn
                    record={record}
                    setFocusExecutionUrn={setFocusExecutionUrn}
                    onExecute={onExecute}
                    onCancel={onCancelExecution}
                    onDelete={onDelete}
                    onView={onView}
                    onEdit={onEdit}
                    navigateToRunHistory={navigateToRunHistory}
                />
            ),
            width: '10%',
        },
    ];

    const handleSortColumnChange = ({ sortColumn, sortOrder }) => {
        onChangeSort(sortColumn, sortOrder);
    };

    return (
        <StyledTable
            columns={tableColumns}
            data={tableData}
            isScrollable
            handleSortColumnChange={handleSortColumnChange}
            isLoading={isLoading}
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
