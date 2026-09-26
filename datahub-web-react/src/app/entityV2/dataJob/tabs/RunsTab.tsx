import { Pagination, Table, Text, Tooltip } from '@components';
import { ArrowSquareOut } from '@phosphor-icons/react/dist/csr/ArrowSquareOut';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { DefaultTheme, useTheme } from 'styled-components';

import { Column } from '@components/components/Table/types';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { notEmpty } from '@app/entityV2/shared/utils';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingest/source/utils';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { scrollToTop } from '@app/shared/searchUtils';
import { safeUrl } from '@app/shared/urlUtils';

import { useGetExecutionRunsQuery } from '@graphql/runs.generated';
import { DataProcessInstanceRunResultType, DataProcessRunStatus, Entity } from '@types';

import LoadingSvg from '@images/datahub-logo-color-loading_pendulum.svg?react';

const ExternalUrlLink = styled.a`
    font-size: 16px;
    color: ${(props) => props.theme.colors.textTertiary};
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

function getStatusForStyling(status?: DataProcessRunStatus, resultType?: DataProcessInstanceRunResultType) {
    if (status === 'COMPLETE') {
        if (resultType === 'SKIPPED') {
            return 'CANCELLED';
        }
        return resultType;
    }
    return 'RUNNING';
}

const PAGE_SIZE = 20;

type RunRow = {
    time?: number | null;
    name?: string | null;
    status?: DataProcessRunStatus | null;
    resultType?: DataProcessInstanceRunResultType | null;
    inputs?: Entity[];
    outputs?: Entity[];
    externalUrl?: string | null;
};

export const RunsTab = () => {
    const { urn } = useEntityData();
    const [page, setPage] = useState(1);
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');

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
            title: t('shared.runIdColumn'),
            key: 'name',
            render: (record) => <div data-testid={`run-name-${record.name}`}>{record.name}</div>,
        },
        {
            title: tl('status'),
            key: 'status',
            render: (record) => {
                const statusForStyling = getStatusForStyling(
                    record.status ?? undefined,
                    record.resultType ?? undefined,
                );
                const text = getExecutionRequestStatusDisplayText(statusForStyling);
                const color = getExecutionRequestStatusDisplayColor(theme, statusForStyling);
                return (
                    <div data-testid={`run-status-${record.name}`}>
                        <div style={{ display: 'flex', justifyContent: 'left', alignItems: 'center' }}>
                            <LastRunIcon
                                theme={theme}
                                status={record.status ?? undefined}
                                resultType={record.resultType ?? undefined}
                            />
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
                    <CompactEntityNameList entities={record.inputs ?? []} placement="right" />
                </div>
            ),
            width: '150px',
        },
        {
            title: t('shared.outputs'),
            key: 'outputs',
            render: (record) => (
                <div data-testid="run-outputs-cell">
                    <CompactEntityNameList entities={record.outputs ?? []} placement="right" />
                </div>
            ),
            width: '150px',
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

    const { loading, data } = useGetExecutionRunsQuery({
        variables: {
            urn,
            start: (page - 1) * PAGE_SIZE,
            count: PAGE_SIZE,
        },
    });
    const runsData = data?.entity && ('runs' in data?.entity ? data?.entity?.runs : null);
    const runs = runsData?.runs;

    const tableData = runs
        ?.filter((run) => run?.state?.length)
        .map((run) => ({
            key: run?.name,
            time: run?.created?.time,
            name: run?.name,
            status: run?.state?.[0]?.status,
            resultType: run?.state?.[0]?.result?.resultType,
            inputs: run?.inputs?.relationships?.map((relationship) => relationship.entity).filter(notEmpty),
            outputs: run?.outputs?.relationships?.map((relationship) => relationship.entity).filter(notEmpty),
            externalUrl: run?.externalUrl,
        }));
    if (loading) {
        return (
            <LoadingContainer>
                <LoadingSvg height={80} width={80} />
                <LoadingText>{t('shared.fetchingRuns')}</LoadingText>
            </LoadingContainer>
        );
    }

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    return (
        <>
            <Table data={tableData ?? []} columns={columns} />
            <PaginationControlContainer>
                <Pagination
                    currentPage={page}
                    itemsPerPage={PAGE_SIZE}
                    total={runsData?.total || 0}
                    showLessItems
                    onPageChange={onChangePage}
                    showSizeChanger={false}
                />
            </PaginationControlContainer>
        </>
    );
};

interface LastRunIconProps {
    theme: DefaultTheme;
    status?: DataProcessRunStatus;
    resultType?: DataProcessInstanceRunResultType;
    showTooltip?: boolean;
}

export function LastRunIcon({ theme, status, resultType, showTooltip }: LastRunIconProps): JSX.Element {
    const statusForStyling = getStatusForStyling(status, resultType);
    const text = getExecutionRequestStatusDisplayText(statusForStyling);
    const Icon = getExecutionRequestStatusIcon(statusForStyling);
    const color = getExecutionRequestStatusDisplayColor(theme, statusForStyling);

    const icon = Icon && <Icon style={{ color, fontSize: 'inherit' }} />;

    return showTooltip ? <Tooltip title={text}>{icon}</Tooltip> : <>{icon}</>;
}
