import { Column, Table, Text, colors } from '@components';
import { SorterResult } from 'antd/lib/table/interface';
import * as QueryString from 'query-string';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import { getDisplayablePoolId } from '@app/ingestV2/executor_saas/utils';
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
import { getSourceStatus } from '@app/ingestV2/source/utils';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import filtersToQueryStringParams from '@app/searchV2/utils/filtersToQueryStringParams';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { IngestionSource } from '@types';

const StyledTable = styled(Table)`
    table-layout: fixed;
` as typeof Table;

const LinkButton = styled(Text)`
    border: none;
    background: none;
    padding: 0;
    cursor: pointer;
    color: ${colors.violet[500]};
    display: inline-block;
`;

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
    sourcesToRefetch: Set<string>;
    executedUrns: Set<string>;
    saasProps: {
        onViewPool: (poolId: string) => void;
    };
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
    sourcesToRefetch,
    executedUrns,
    saasProps,
}: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();
    const appConfig = useAppConfig();
    const isPoolsDisplayEnabled = appConfig.config.featureFlags.displayExecutorPools;

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
        executorPoolId: source.config?.executorId, // SaaS only
        privileges: source.privileges,
    }));

    const tableColumns: Column<IngestionSourceTableData>[] = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return <NameColumn type={record.type} record={record} />;
            },
            width: '20%',
            sorter: true,
        },
        {
            title: 'Schedule',
            key: 'schedule',
            render: (record) => <ScheduleColumn schedule={record.schedule || ''} timezone={record.timezone || ''} />,
            width: '15%',
        },
        ...(isPoolsDisplayEnabled
            ? [
                  {
                      // SaaS only
                      title: 'Executor Pool',
                      key: 'executor-pool',
                      render: (record) =>
                          record.executorPoolId && !record.cliIngestion ? (
                              <LinkButton
                                  onClick={(e) => {
                                      e.stopPropagation();
                                      saasProps.onViewPool(record.executorPoolId);
                                  }}
                              >
                                  {getDisplayablePoolId({ executorPoolId: record.executorPoolId })}
                              </LinkButton>
                          ) : (
                              <span>{record.cliIngestion ? 'N/A' : 'Unknown'}</span>
                          ),
                      width: '15%',
                  },
              ]
            : []),
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
            width: '10%',
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
