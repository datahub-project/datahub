import React from 'react';
import styled from 'styled-components';
import { Checkbox, Empty } from 'antd';
import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { StyledTable } from '../../../components/styled/StyledTable';
import { Assertion, AssertionRunStatus, MonitorMode, DataContract } from '../../../../../../types.generated';
import { canManageAssertionMonitor, getEntityUrnForAssertion, getNextScheduleEvaluationTimeMs } from './acrylUtils';
import { getDataContractCategoryFromAssertion } from './contract/utils';
import { useIngestionSourceForEntityQuery } from '../../../../../../graphql/ingestion.generated';
import { useEntityData } from '../../../EntityContext';
import { AcrylAssertionDetails } from './AcrylAssertionDetails';
import { ActionsColumn, DetailsColumn } from './AcrylAssertionsTableColumns';
import { DataContractCategoryType } from './contract/builder/types';

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

const DetailsColumnWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const AssertionSelectCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;

type Props = {
    assertions: Array<Assertion>;
    contract?: DataContract;
    showMenu?: boolean;
    showDetails?: boolean;
    showSelect?: boolean;
    selectedUrns?: string[];
    onDeleteAssertion: (urn: string) => void;
    onManageAssertion: (urn: string) => void;
    onViewAssertionDetails: (urn: string) => void;
    onStartMonitor: (assertionUrn: string, monitorUrn: string) => void;
    onStopMonitor: (assertionUrn: string, monitorUrn: string) => void;
    onAddToContract?: (category: DataContractCategoryType, entityUrn: string, assertionUrn: string) => void;
    onRemoveFromContract?: (entityUrn: string, assertionUrn: string) => void;
    onSelect?: (assertionUrn: string) => void;
};

/**
 * Acryl-specific list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const AcrylAssertionsTable = ({
    assertions,
    contract,
    showMenu = true,
    showDetails = true,
    showSelect = false,
    selectedUrns,
    onDeleteAssertion,
    onManageAssertion,
    onViewAssertionDetails,
    onStartMonitor,
    onStopMonitor,
    onAddToContract,
    onRemoveFromContract,
    onSelect,
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
                const selected = selectedUrns?.some((selectedUrn) => selectedUrn === record.urn);
                return (
                    <DetailsColumnWrapper>
                        {showSelect && (
                            <AssertionSelectCheckbox
                                checked={selected}
                                onChange={() => onSelect?.(record.urn as string)}
                            />
                        )}
                        <DetailsColumn
                            assertion={record.assertion}
                            monitor={record.monitor}
                            contract={contract}
                            lastEvaluationTimeMs={record.lastEvaluationTimeMs}
                            lastEvaluationResult={record.lastEvaluationResult}
                            onViewAssertionDetails={() => onViewAssertionDetails(record.urn)}
                        />
                    </DetailsColumnWrapper>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                return (
                    <>
                        {(showMenu && (
                            <ActionsColumn
                                assertion={record.assertion}
                                platform={record.platform}
                                monitor={record.monitor}
                                canManageAssertion={canManageAssertionMonitor(
                                    record.monitor,
                                    connectionForEntityExists,
                                )}
                                contract={contract}
                                lastEvaluationUrl={record.lastEvaluationUrl}
                                onManageAssertion={() => onManageAssertion(record.urn)}
                                onDeleteAssertion={() => onDeleteAssertion(record.urn)}
                                onStartMonitor={
                                    record.monitor && (() => onStartMonitor(record.urn, record.monitor.urn))
                                }
                                onStopMonitor={record.monitor && (() => onStopMonitor(record.urn, record.monitor.urn))}
                                onAddToContract={() =>
                                    onAddToContract?.(
                                        getDataContractCategoryFromAssertion(record.assertion),
                                        getEntityUrnForAssertion(record.assertion) as string,
                                        record.urn,
                                    )
                                }
                                onRemoveFromContract={() =>
                                    onRemoveFromContract?.(
                                        getEntityUrnForAssertion(record.assertion) as string,
                                        record.urn,
                                    )
                                }
                            />
                        )) ||
                            undefined}
                    </>
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
            expandable={
                (showDetails && {
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
                }) ||
                undefined
            }
            showHeader={false}
            pagination={false}
        />
    );
};
