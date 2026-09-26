import { Pagination, Table, Text, Tooltip } from '@components';
import { ArrowSquareOut } from '@phosphor-icons/react/dist/csr/ArrowSquareOut';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Column } from '@components/components/Table/types';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { notEmpty } from '@app/entityV2/shared/utils';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingest/source/utils';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { formatDuration } from '@app/shared/formatDuration';
import { scrollToTop } from '@app/shared/searchUtils';
import { safeUrl } from '@app/shared/urlUtils';

import { GetDatasetRunsQuery, useGetDatasetRunsQuery } from '@graphql/dataset.generated';
import {
    DataProcessInstanceRunResultType,
    DataProcessRunStatus,
    Entity,
    EntityType,
    RelationshipDirection,
} from '@types';

import LoadingSvg from '@images/datahub-logo-color-loading_pendulum.svg?react';

const ExternalUrlLink = styled.a`
    font-size: 16px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const PaginationControlContainer = styled.div`
    padding-top: 16px;
    padding-bottom: 16px;
    text-align: center;
`;

const LoadingText = styled.div`
    margin-top: 18px;
    font-size: 12px;
`;

const LoadingContainer = styled.div`
    padding-top: 40px;
    padding-bottom: 40px;
    width: 100%;
    text-align: center;
`;

function getStatusForStyling(
    status?: DataProcessRunStatus | null,
    resultType?: DataProcessInstanceRunResultType | null,
) {
    if (status === 'COMPLETE') {
        if (resultType === 'SKIPPED') {
            return 'CANCELLED';
        }
        return resultType ?? undefined;
    }
    return 'RUNNING';
}

const PAGE_SIZE = 20;

type RunRow = {
    time?: number | null;
    duration?: number | null;
    name?: string | null;
    parentTemplate?: Entity;
    status?: DataProcessRunStatus | null;
    resultType?: DataProcessInstanceRunResultType | null;
    inputs?: Entity[];
    outputs?: Entity[];
    externalUrl?: string | null;
};

export const OperationsTab = () => {
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');
    const { urn, entityData } = useEntityData();
    const [page, setPage] = useState(1);

    const theme = useTheme();

    const columns: Column<RunRow>[] = [
        {
            title: t('shared.timeColumn'),
            key: 'time',
            render: (record) => (
                <Tooltip title={new Date(Number(record.time)).toUTCString()}>
                    {new Date(Number(record.time)).toLocaleString()}
                </Tooltip>
            ),
        },
        {
            title: t('shared.durationColumn'),
            key: 'duration',
            render: (record) => formatDuration(record.duration ?? 0),
        },
        {
            title: t('shared.runIdColumn'),
            key: 'name',
            render: (record) => <div data-testid={`run-name-${record.name}`}>{record.name}</div>,
        },
        {
            title: t('dataJob.name'),
            key: 'parentTemplate',
            render: (record) => (
                <CompactEntityNameList entities={record.parentTemplate ? [record.parentTemplate] : []} />
            ),
        },
        {
            title: tl('status'),
            key: 'status',
            render: (record) => {
                const statusForStyling = getStatusForStyling(record.status, record.resultType);
                const Icon = getExecutionRequestStatusIcon(statusForStyling);
                const text = getExecutionRequestStatusDisplayText(statusForStyling);
                const color = getExecutionRequestStatusDisplayColor(theme, statusForStyling);
                return (
                    <div data-testid={`run-status-${record.name}`}>
                        <div style={{ display: 'flex', justifyContent: 'left', alignItems: 'center' }}>
                            {Icon && <Icon style={{ color }} />}
                            <Text weight="bold" style={{ color, marginLeft: 8 }}>
                                {text || tl('na')}
                            </Text>
                        </div>
                    </div>
                );
            },
        },
        {
            title: t('shared.inputs'),
            key: 'inputs',
            render: (record) => (
                <div data-testid="run-inputs-cell">
                    <CompactEntityNameList entities={record.inputs ?? []} />
                </div>
            ),
        },
        {
            title: t('shared.outputs'),
            key: 'outputs',
            render: (record) => (
                <div data-testid="run-outputs-cell">
                    <CompactEntityNameList entities={record.outputs ?? []} />
                </div>
            ),
        },
        {
            title: '',
            key: 'externalUrl',
            render: (record) =>
                record.externalUrl && (
                    <Tooltip title={t('shared.viewTaskRunDetails')}>
                        <ExternalUrlLink href={safeUrl(record.externalUrl)}>
                            <ArrowSquareOut />
                        </ExternalUrlLink>
                    </Tooltip>
                ),
        },
    ];

    // Fetch data across all siblings.
    const allUrns = [
        urn,
        ...(entityData?.siblingsSearch?.searchResults || []).map((sibling) => sibling.entity.urn).filter(notEmpty),
    ];
    const loadings: boolean[] = [];
    const datas: GetDatasetRunsQuery[] = [];
    allUrns.forEach((entityUrn) => {
        // Because there's a consistent number and order of the urns,
        // this usage of a hook within a loop should be safe.
        // eslint-disable-next-line react-hooks/rules-of-hooks
        const { loading, data } = useGetDatasetRunsQuery({
            variables: {
                urn: entityUrn,
                start: (page - 1) * PAGE_SIZE,
                count: PAGE_SIZE,
                direction: RelationshipDirection.Outgoing,
            },
        });
        loadings.push(loading);
        if (data) {
            datas.push(data);
        }
    });

    const loading = loadings.some((loadingEntry) => loadingEntry);

    // Merge the runs data from all entities.
    // If there's more than one entity contributing to the data, then we can't do pagination.
    let canPaginate = true;
    let dataRuns: NonNullable<GetDatasetRunsQuery['dataset']>['runs'] | undefined;
    if (datas.length > 0) {
        let numWithRuns = 0;
        for (let i = 0; i < datas.length; i++) {
            if (datas[i]?.dataset?.runs?.total) {
                numWithRuns++;
            }

            if (dataRuns && dataRuns.runs) {
                dataRuns.runs.push(...(datas[i]?.dataset?.runs?.runs || []));
                dataRuns.total = (dataRuns.total ?? 0) + (datas[i]?.dataset?.runs?.total ?? 0);
            } else {
                dataRuns = JSON.parse(JSON.stringify(datas[i]?.dataset?.runs));
            }
        }

        if (numWithRuns > 1) {
            canPaginate = false;
        }
    }

    // This also sorts the runs data across all entities.
    const runs = dataRuns?.runs?.sort((a, b) => (b?.created?.time ?? 0) - (a?.created?.time ?? 0));

    const tableData = runs
        ?.filter((run) => run)
        .map((run) => ({
            key: run?.name,
            time: run?.created?.time,
            name: run?.name,
            status: run?.state?.[0]?.status,
            resultType: run?.state?.[0]?.result?.resultType,
            duration: run?.state?.[0]?.durationMillis,
            inputs: run?.inputs?.relationships?.map((relationship) => relationship.entity).filter(notEmpty),
            outputs: run?.outputs?.relationships?.map((relationship) => relationship.entity).filter(notEmpty),
            externalUrl: run?.externalUrl,
            parentTemplate: run?.parentTemplate?.relationships?.[0]?.entity ?? undefined,
        }));

    // If the table contains jobs, we need to show the job-related columns. Otherwise we can simplify the table.
    const containsJobs = tableData?.some((run) => run.parentTemplate?.type !== EntityType.Dataset);
    const simplifiedColumns = containsJobs
        ? columns
        : columns.filter((column) => !['name', 'inputs', 'outputs'].includes(column.key));

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    // TODO: Much of this file is duplicated from RunsTab.tsx. We should refactor this to share code.
    return (
        <>
            {loading && (
                <LoadingContainer>
                    <LoadingSvg height={80} width={80} />
                    <LoadingText>{t('shared.fetchingRuns')}</LoadingText>
                </LoadingContainer>
            )}
            {!loading && (
                <>
                    <Table data={tableData ?? []} columns={simplifiedColumns} />
                    {canPaginate && (
                        <PaginationControlContainer>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={PAGE_SIZE}
                                total={dataRuns?.total || 0}
                                showLessItems
                                onPageChange={onChangePage}
                                showSizeChanger={false}
                            />
                        </PaginationControlContainer>
                    )}
                </>
            )}
        </>
    );
};
