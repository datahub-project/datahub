import { DeliveredProcedureOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Pagination, Table, Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { DefaultTheme, useTheme } from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '@app/ingest/source/utils';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { scrollToTop } from '@app/shared/searchUtils';
import { safeUrl } from '@app/shared/urlUtils';

import { useGetExecutionRunsQuery } from '@graphql/runs.generated';
import { DataProcessInstanceRunResultType, DataProcessRunStatus } from '@types';

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

export const RunsTab = () => {
    const { urn } = useEntityData();
    const [page, setPage] = useState(1);
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');

    const theme = useTheme();

    const columns = [
        {
            title: t('shared.timeColumn'),
            dataIndex: 'time',
            key: 'time',
            render: (value) => (
                <Tooltip title={new Date(Number(value)).toUTCString()}>
                    {new Date(Number(value)).toLocaleString()}
                </Tooltip>
            ),
        },
        {
            title: t('shared.runIdColumn'),
            dataIndex: 'name',
            key: 'name',
            render: (name) => <div data-testid={`run-name-${name}`}>{name}</div>,
        },
        {
            title: tl('status'),
            dataIndex: 'status',
            key: 'status',
            render: (status: any, row) => {
                const statusForStyling = getStatusForStyling(status, row?.resultType);
                const text = getExecutionRequestStatusDisplayText(statusForStyling);
                const color = getExecutionRequestStatusDisplayColor(theme, statusForStyling);
                return (
                    <div data-testid={`run-status-${row.name}`}>
                        <div style={{ display: 'flex', justifyContent: 'left', alignItems: 'center' }}>
                            <LastRunIcon theme={theme} status={status} resultType={row?.resultType} />
                            <Typography.Text strong style={{ color, marginLeft: 8 }}>
                                {text || tl('na')}
                            </Typography.Text>
                        </div>
                    </div>
                );
            },
        },
        {
            title: t('shared.inputs'),
            dataIndex: 'inputs',
            key: 'inputs',
            render: (inputs) => <CompactEntityNameList entities={inputs} placement="right" />,
            width: 150,
        },
        {
            title: t('shared.outputs'),
            dataIndex: 'outputs',
            key: 'outputs',
            render: (outputs) => <CompactEntityNameList entities={outputs} placement="right" />,
            width: 150,
        },
        {
            title: '',
            dataIndex: 'externalUrl',
            key: 'externalUrl',
            render: (externalUrl) =>
                externalUrl && (
                    <Tooltip title={t('shared.viewTaskRunDetails')}>
                        <ExternalUrlLink href={safeUrl(externalUrl)}>
                            <DeliveredProcedureOutlined />
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
            inputs: run?.inputs?.relationships?.map((relationship) => relationship.entity),
            outputs: run?.outputs?.relationships?.map((relationship) => relationship.entity),
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
            <Table dataSource={tableData} columns={columns} pagination={false} />
            <PaginationControlContainer>
                <Pagination
                    current={page}
                    pageSize={PAGE_SIZE}
                    total={runsData?.total || 0}
                    showLessItems
                    onChange={onChangePage}
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
