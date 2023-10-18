import React from 'react';
import styled from 'styled-components';
import { Empty } from 'antd';
import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { StyledTable } from '../../../components/styled/StyledTable';
import { Assertion, AssertionRunStatus, MonitorMode } from '../../../../../../types.generated';
import { useIngestionSourceForEntityQuery } from '../../../../../../graphql/ingestion.generated';
import { useEntityData } from '../../../EntityContext';
import { AcrylAssertionDetails } from './AcrylAssertionDetails';
import { canManageAssertionMonitor, getNextScheduleEvaluationTimeMs } from './acrylUtils';
import { ActionsColumn, DetailsColumn } from './AcrylAssertionsTableColumns';

const AssertionContainer = styled.div`
    padding-top: 12px;
    padding-bottom: 12px;
`;

const StyledStyledTable = styled(StyledTable)`
    && {
        .ant-table-tbody > tr > td {
            border: none;
        }
    }
` as typeof StyledTable;

const StyledDownOutlined = styled(DownOutlined)`
    font-size: 8px;
`;

const StyledRightOutlined = styled(RightOutlined)`
    font-size: 8px;
`;

type Props = {
    assertions: Array<Assertion>;
    onDeleteAssertion: (urn: string) => void;
    onManageAssertion: (urn: string) => void;
    onViewAssertionDetails: (urn: string) => void;
    onStartMonitor: (assertionUrn: string, monitorUrn: string) => void;
    onStopMonitor: (assertionUrn: string, monitorUrn: string) => void;
};

/**
 * Acryl-specific list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const AcrylAssertionsTable = ({
    assertions,
    onDeleteAssertion,
    onManageAssertion,
    onViewAssertionDetails,
    onStartMonitor,
    onStopMonitor,
}: Props) => {
    const { urn } = useEntityData();
    const { data: ingestionSourceData } = useIngestionSourceForEntityQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
        skip: !urn,
    });
    const connectionForEntityExists = !!ingestionSourceData?.ingestionSourceForEntity?.urn;

    const assertionsTableData = assertions.map((assertion) => ({
        urn: assertion.urn,
        type: assertion.info?.type,
        platform: assertion.platform,
        datasetAssertionInfo: assertion.info?.datasetAssertion,
        freshnessAssertionInfo: assertion.info?.freshnessAssertion,
        lastEvaluationTimeMs:
            assertion.runEvents?.runEvents?.length && assertion.runEvents.runEvents[0].timestampMillis,
        lastEvaluationResult:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.type,
        lastEvaluationUrl:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.externalUrl,
        assertion,
        monitor:
            (assertion as any).monitor?.relationships?.length && (assertion as any).monitor?.relationships[0].entity,
    }));

    const assertionsTableCols = [
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                return (
                    <DetailsColumn
                        assertion={record.assertion}
                        monitor={record.monitor}
                        lastEvaluationTimeMs={record.lastEvaluationTimeMs}
                        lastEvaluationResult={record.lastEvaluationResult}
                        onViewAssertionDetails={() => onViewAssertionDetails(record.urn)}
                    />
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                return (
                    <ActionsColumn
                        platform={record.platform}
                        monitor={record.monitor}
                        canManageAssertion={canManageAssertionMonitor(record.monitor, connectionForEntityExists)}
                        lastEvaluationUrl={record.lastEvaluationUrl}
                        onManageAssertion={() => onManageAssertion(record.urn)}
                        onDeleteAssertion={() => onDeleteAssertion(record.urn)}
                        onStartMonitor={record.monitor && (() => onStartMonitor(record.urn, record.monitor.urn))}
                        onStopMonitor={record.monitor && (() => onStopMonitor(record.urn, record.monitor.urn))}
                    />
                );
            },
        },
    ];

    return (
        <StyledStyledTable
            columns={assertionsTableCols}
            dataSource={assertionsTableData}
            rowKey="urn"
            locale={{
                emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            expandable={{
                defaultExpandAllRows: false,
                expandRowByClick: true,
                expandedRowRender: (record) => {
                    const isStopped = record.monitor?.info?.status?.mode === MonitorMode.Inactive;
                    const schedule =
                        record.monitor?.info?.assertionMonitor?.assertions?.length &&
                        record.monitor?.info?.assertionMonitor?.assertions[0].schedule;
                    const nextEvaluatedAtMillis = schedule && getNextScheduleEvaluationTimeMs(schedule);
                    return (
                        <AssertionContainer>
                            <AcrylAssertionDetails
                                urn={record.urn}
                                isStopped={isStopped}
                                schedule={schedule}
                                lastEvaluatedAtMillis={record.lastEvaluationTimeMs}
                                nextEvaluatedAtMillis={nextEvaluatedAtMillis}
                            />
                        </AssertionContainer>
                    );
                },
                expandIcon: ({ expanded, onExpand, record }: any) =>
                    expanded ? (
                        <StyledDownOutlined onClick={(e) => onExpand(record, e)} />
                    ) : (
                        <StyledRightOutlined onClick={(e) => onExpand(record, e)} />
                    ),
            }}
            showHeader={false}
            pagination={false}
        />
    );
};
