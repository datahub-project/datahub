import React, { useState } from 'react';
import styled from 'styled-components';
import { Checkbox, Empty, Table, TableProps } from 'antd';
import { Assertion, AssertionRunStatus, DataContract } from '../../../../../../types.generated';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { ActionsColumn, DetailsColumn } from './AcrylAssertionsTableColumns';
import { AssertionProfileDrawer } from './assertion/profile/AssertionProfileDrawer';
import { ANTD_GRAY } from '../../../constants';
import { useOpenAssertionDetailModal } from './assertion/builder/hooks';
import { getEntityUrnForAssertion, getSiblingWithUrn } from './acrylUtils';

type StyledTableProps = {
    showSelect?: boolean;
} & TableProps<any>;

export const StyledTable = styled(Table)<StyledTableProps>`
    ${(props) => !props.showSelect && `margin-left: -50px;`}
    max-width: none;
    overflow: inherit;
    height: inherit;
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: ${ANTD_GRAY[8]};
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${ANTD_GRAY[4]};
    }
    && {
        .ant-table-tbody > tr > td {
            border: none;
            ${(props) => props.showSelect && `padding: 16px 20px;`}
        }
    }
    &&& .ant-table-cell {
        background-color: transparent;
    }
    &&& .acryl-assertions-table-row {
        cursor: pointer;
        background-color: ${ANTD_GRAY[2]};
        :hover {
            background-color: ${ANTD_GRAY[3]};
        }
    }
    &&& .acryl-selected-assertions-table-row {
        background-color: ${ANTD_GRAY[4]};
    }
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
    showSelect?: boolean;
    selectedUrns?: string[];
    onSelect?: (assertionUrn: string) => void;
    refetch?: () => void;
};

/**
 * Acryl-specific list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 */
export const AcrylAssertionsTable = ({
    assertions,
    contract,
    showMenu = true,
    showSelect = false,
    selectedUrns,
    onSelect,
    refetch,
}: Props) => {
    const { entityData } = useEntityData();
    const [focusAssertionUrn, setFocusAssertionUrn] = useState<string | null>(null);

    const focusedAssertion = assertions.find((assertion) => assertion.urn === focusAssertionUrn);
    const focusedEntityUrn = focusedAssertion ? getEntityUrnForAssertion(focusedAssertion) : undefined;
    const focusedAssertionEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;

    if (focusAssertionUrn && !focusedAssertion) {
        setFocusAssertionUrn(null);
    }

    useOpenAssertionDetailModal(setFocusAssertionUrn);

    const assertionsTableData = assertions.map((assertion) => ({
        urn: assertion.urn,
        type: assertion.info?.type,
        platform: assertion.platform,
        datasetAssertionInfo: assertion.info?.datasetAssertion,
        freshnessAssertionInfo: assertion.info?.freshnessAssertion,
        lastEvaluation:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0],
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
            (assertion as any).monitor?.relationships?.length && (assertion as any).monitor?.relationships[0]?.entity,
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
                                onClick={(e) => e.stopPropagation()}
                                onChange={() => onSelect?.(record.urn as string)}
                            />
                        )}
                        <DetailsColumn
                            assertion={record.assertion}
                            contract={contract}
                            lastEvaluation={record.lastEvaluation}
                            onViewAssertionDetails={() => setFocusAssertionUrn(record.urn)}
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
                // TODO: Add permission for editing contract.
                return (
                    <>
                        {(showMenu && (
                            <ActionsColumn
                                assertion={record.assertion}
                                contract={contract}
                                canEditContract
                                refetch={refetch}
                            />
                        )) ||
                            undefined}
                    </>
                );
            },
        },
    ];

    return (
        <>
            <StyledTable
                showSelect={showSelect}
                rowClassName={(record) => {
                    return (
                        (record.urn === focusAssertionUrn && 'acryl-selected-assertions-table-row') ||
                        'acryl-assertions-table-row'
                    );
                }}
                columns={assertionsTableCols}
                dataSource={assertionsTableData}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                onRow={(record) => {
                    return {
                        onClick: (_) => {
                            if (showSelect) {
                                onSelect?.(record.urn as string);
                            } else {
                                setFocusAssertionUrn(record.urn);
                            }
                        },
                    };
                }}
                showHeader={false}
                pagination={false}
            />
            {focusAssertionUrn && focusedAssertionEntity && (
                <AssertionProfileDrawer
                    urn={focusAssertionUrn}
                    contract={contract}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                    refetch={refetch}
                />
            )}
        </>
    );
};
