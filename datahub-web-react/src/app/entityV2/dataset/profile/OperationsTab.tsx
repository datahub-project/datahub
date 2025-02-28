import { DeliveredProcedureOutlined } from '@ant-design/icons';
import { Pagination, Table, Typography } from 'antd';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { GetDatasetRunsQuery, useGetDatasetRunsQuery } from '../../../../graphql/dataset.generated';
import {
    DataProcessInstanceRunResultType,
    DataProcessRunStatus,
    EntityType,
    RelationshipDirection,
} from '../../../../types.generated';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from '../../../ingest/source/utils';
import { CompactEntityNameList } from '../../../recommendations/renderer/component/CompactEntityNameList';
import { ANTD_GRAY } from '../../shared/constants';
import { useEntityData } from '../../../entity/shared/EntityContext';
import LoadingSvg from '../../../../images/datahub-logo-color-loading_pendulum.svg?react';
import { scrollToTop } from '../../../shared/searchUtils';
import { formatDuration } from '../../../shared/formatDuration';
import { notEmpty } from '../../shared/utils';

const ExternalUrlLink = styled.a`
    font-size: 16px;
    color: ${ANTD_GRAY[8]};
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

function getStatusForStyling(status: DataProcessRunStatus, resultType: DataProcessInstanceRunResultType) {
    if (status === 'COMPLETE') {
        if (resultType === 'SKIPPED') {
            return 'CANCELLED';
        }
        return resultType;
    }
    return 'RUNNING';
}

const columns = [
    {
        title: 'Time',
        dataIndex: 'time',
        key: 'time',
        render: (value) => (
            <Tooltip title={new Date(Number(value)).toUTCString()}>{new Date(Number(value)).toLocaleString()}</Tooltip>
        ),
    },
    {
        title: 'Duration',
        dataIndex: 'duration',
        key: 'duration',
        render: (durationMs: number) => formatDuration(durationMs),
    },
    {
        title: 'Run ID',
        dataIndex: 'name',
        key: 'name',
    },
    {
        title: 'Task',
        dataIndex: 'parentTemplate',
        key: 'parentTemplate',
        render: (parentTemplate) => <CompactEntityNameList entities={[parentTemplate]} />,
    },
    {
        title: 'Status',
        dataIndex: 'status',
        key: 'status',
        render: (status: any, row) => {
            const statusForStyling = getStatusForStyling(status, row?.resultType);
            const Icon = getExecutionRequestStatusIcon(statusForStyling);
            const text = getExecutionRequestStatusDisplayText(statusForStyling);
            const color = getExecutionRequestStatusDisplayColor(statusForStyling);
            return (
                <>
                    <div style={{ display: 'flex', justifyContent: 'left', alignItems: 'center' }}>
                        {Icon && <Icon style={{ color }} />}
                        <Typography.Text strong style={{ color, marginLeft: 8 }}>
                            {text || 'N/A'}
                        </Typography.Text>
                    </div>
                </>
            );
        },
    },
    {
        title: 'Inputs',
        dataIndex: 'inputs',
        key: 'inputs',
        render: (inputs) => <CompactEntityNameList entities={inputs} />,
    },
    {
        title: 'Outputs',
        dataIndex: 'outputs',
        key: 'outputs',
        render: (outputs) => <CompactEntityNameList entities={outputs} />,
    },
    {
        title: '',
        dataIndex: 'externalUrl',
        key: 'externalUrl',
        render: (externalUrl) =>
            externalUrl && (
                <Tooltip title="View task run details">
                    <ExternalUrlLink href={externalUrl}>
                        <DeliveredProcedureOutlined />
                    </ExternalUrlLink>
                </Tooltip>
            ),
    },
];

const PAGE_SIZE = 20;

export const OperationsTab = () => {
    const { urn, entityData } = useEntityData();
    const [page, setPage] = useState(1);

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
            time: run?.created?.time,
            name: run?.name,
            status: run?.state?.[0]?.status,
            resultType: run?.state?.[0]?.result?.resultType,
            duration: run?.state?.[0]?.durationMillis,
            inputs: run?.inputs?.relationships?.map((relationship) => relationship.entity),
            outputs: run?.outputs?.relationships?.map((relationship) => relationship.entity),
            externalUrl: run?.externalUrl,
            parentTemplate: run?.parentTemplate?.relationships?.[0]?.entity,
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
                    <LoadingText>Fetching runs...</LoadingText>
                </LoadingContainer>
            )}
            {!loading && (
                <>
                    <Table dataSource={tableData} columns={simplifiedColumns} pagination={false} />
                    {canPaginate && (
                        <PaginationControlContainer>
                            <Pagination
                                current={page}
                                pageSize={PAGE_SIZE}
                                total={dataRuns?.total || 0}
                                showLessItems
                                onChange={onChangePage}
                                showSizeChanger={false}
                            />
                        </PaginationControlContainer>
                    )}
                </>
            )}
        </>
    );
};
