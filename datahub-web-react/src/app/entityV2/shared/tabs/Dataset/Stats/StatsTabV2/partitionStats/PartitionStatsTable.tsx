import { Pagination } from 'antd';
import { Table, Text } from '@components';
import { SortingState } from '@components/components/Table/types';
import { getSortedData } from '@components/components/Table/utils';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import usePagination from '@app/sharedV2/pagination/usePagination';
import { FULL_TABLE_PARTITION_KEYS } from '@app/entityV2/shared/tabs/Dataset/Stats/constants';
import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { formatBytes } from '@app/shared/formatNumber';
import { useGetLatestPartitionProfilesLazyQuery } from '@graphql/dataset.generated';
import { countFormatter } from '@utils/formatter/index';

const SectionContainer = styled.div`
    border-bottom: 1px solid var(--gray-3);
    padding: 16px 20px;
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-top: 12px;
`;

const EMPTY_TABLE_MESSAGE = 'No partition stats collected for this table yet.';
const DEFAULT_PAGE_SIZE = 50;

const formatEpochHour = (hours: number) => {
    const ms = hours * 60 * 60 * 1000;
    const date = new Date(ms);
    return date.toLocaleString(undefined, {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
        timeZoneName: 'short',
    });
};

const formatEpochDay = (days: number) => {
    const ms = days * 24 * 60 * 60 * 1000;
    const date = new Date(ms);
    return date.toLocaleDateString(undefined, {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    });
};

const formatEpochMonth = (months: number) => {
    const year = 1970 + Math.floor(months / 12);
    const month = ((months % 12) + 12) % 12;
    const date = new Date(Date.UTC(year, month, 1));
    return date.toLocaleDateString(undefined, {
        year: 'numeric',
        month: '2-digit',
    });
};

const formatEpochYear = (years: number) => {
    const date = new Date(Date.UTC(1970 + years, 0, 1));
    return date.toLocaleDateString(undefined, { year: 'numeric' });
};

const formatPartitionValue = (key: string, rawValue: string) => {
    const parsed = Number(rawValue);
    if (!Number.isFinite(parsed)) return rawValue;

    if (key.endsWith('_hour')) return formatEpochHour(parsed);
    if (key.endsWith('_day')) return formatEpochDay(parsed);
    if (key.endsWith('_month')) return formatEpochMonth(parsed);
    if (key.endsWith('_year')) return formatEpochYear(parsed);
    return rawValue;
};

const formatPartitionKey = (partition?: string | null) => {
    if (!partition) return 'Unknown';
    const parts = partition.split('/');
    const formattedParts = parts.map((part) => {
        const [key, ...rest] = part.split('=');
        if (!key || rest.length === 0) return part;
        const value = rest.join('=');
        const formattedValue = formatPartitionValue(key, value);
        return `${key}=${formattedValue}`;
    });
    return formattedParts.join('/');
};

const PartitionStatsTable = () => {
    const { statsEntityUrn } = useStatsSectionsContext();
    const [getLatestPartitionProfiles, { data, loading }] = useGetLatestPartitionProfilesLazyQuery();
    const { page, setPage, pageSize, setPageSize, start } = usePagination(DEFAULT_PAGE_SIZE);
    const [sortColumn, setSortColumn] = useState<string | null>(null);
    const [sortOrder, setSortOrder] = useState<SortingState>(SortingState.ORIGINAL);

    useEffect(() => {
        if (!statsEntityUrn) return;
        getLatestPartitionProfiles({
            variables: {
                urn: statsEntityUrn,
            },
        });
    }, [statsEntityUrn, getLatestPartitionProfiles]);

    const partitionProfiles = useMemo(() => {
        const profiles = data?.dataset?.latestPartitionProfiles?.partitionProfiles || [];
        return profiles.filter((profile) => !FULL_TABLE_PARTITION_KEYS.includes(profile.partition || ''));
    }, [data]);

    const tableData = useMemo(
        () =>
            partitionProfiles.map((profile) => ({
                key: profile.partition,
                partition: profile.partition,
                rowCount: profile.rowCount,
                columnCount: profile.columnCount,
                partitionCount: profile.partitionCount,
                sizeInBytes: profile.sizeInBytes,
            })),
        [partitionProfiles],
    );

    const columns = useMemo(
        () => [
            {
                title: 'Partition',
                dataIndex: 'partition',
                key: 'partition',
                render: (row) => formatPartitionKey(row.partition),
                sorter: (a, b) => (a.partition || '').localeCompare(b.partition || ''),
            },
            {
                title: 'Rows',
                dataIndex: 'rowCount',
                key: 'rowCount',
                render: (row) => (row.rowCount !== undefined ? countFormatter(row.rowCount) : '--'),
                sorter: (a, b) => (a.rowCount ?? 0) - (b.rowCount ?? 0),
            },
            {
                title: 'Partitions',
                dataIndex: 'partitionCount',
                key: 'partitionCount',
                render: (row) => (row.partitionCount !== undefined ? countFormatter(row.partitionCount) : '--'),
                sorter: (a, b) => (a.partitionCount ?? 0) - (b.partitionCount ?? 0),
            },
            {
                title: 'Columns',
                dataIndex: 'columnCount',
                key: 'columnCount',
                render: (row) =>
                    row.columnCount !== undefined ? formatNumberWithoutAbbreviation(row.columnCount) : '--',
                sorter: (a, b) => (a.columnCount ?? 0) - (b.columnCount ?? 0),
            },
            {
                title: 'Size',
                dataIndex: 'sizeInBytes',
                key: 'sizeInBytes',
                render: (row) => {
                    if (row.sizeInBytes === undefined) return '--';
                    const formatted = formatBytes(row.sizeInBytes);
                    return `${formatNumberWithoutAbbreviation(formatted.number)} ${formatted.unit}`;
                },
                sorter: (a, b) => (a.sizeInBytes ?? 0) - (b.sizeInBytes ?? 0),
            },
        ],
        [],
    );

    const sortedData = useMemo(
        () => getSortedData(columns, tableData, sortColumn, sortOrder),
        [columns, tableData, sortColumn, sortOrder],
    );
    const totalPartitions = sortedData.length;
    const totalPages = Math.max(1, Math.ceil(totalPartitions / pageSize));
    const pagedData = useMemo(
        () => sortedData.slice(start, start + pageSize),
        [sortedData, start, pageSize],
    );

    useEffect(() => {
        if (page > totalPages) {
            setPage(1);
        }
    }, [page, totalPages, setPage]);

    return (
        <SectionContainer>
            <SectionHeader>
                <Text size="lg" weight="bold">
                    Partition Stats
                </Text>
                <Text size="sm" color="gray">
                    {totalPartitions === 0
                        ? 'Showing 0 partitions'
                        : `Showing ${start + 1}-${Math.min(start + pageSize, totalPartitions)} of ${totalPartitions} partitions`}
                </Text>
            </SectionHeader>
            {tableData.length === 0 && !loading ? (
                <Text size="sm" color="gray">
                    {EMPTY_TABLE_MESSAGE}
                </Text>
            ) : (
                <>
                    <Table
                        data={pagedData}
                        columns={columns}
                        isScrollable
                        maxHeight="350px"
                        handleSortColumnChange={({ sortColumn: nextColumn, sortOrder: nextOrder }) => {
                            setSortColumn(nextColumn);
                            setSortOrder(nextOrder);
                        }}
                    />
                    {totalPartitions > pageSize && (
                        <PaginationContainer>
                            <Pagination
                                current={page}
                                pageSize={pageSize}
                                total={totalPartitions}
                                showSizeChanger
                                pageSizeOptions={[25, 50, 100, 200, 500]}
                                onChange={(nextPage, nextPageSize) => {
                                    setPage(nextPage);
                                    if (nextPageSize !== pageSize) {
                                        setPageSize(nextPageSize);
                                    }
                                }}
                            />
                        </PaginationContainer>
                    )}
                </>
            )}
        </SectionContainer>
    );
};

export default PartitionStatsTable;
