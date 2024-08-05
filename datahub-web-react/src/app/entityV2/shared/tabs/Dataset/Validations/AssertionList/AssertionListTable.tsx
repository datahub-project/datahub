import React, { useState } from 'react';
import styled from 'styled-components';
import { Table, Typography, Empty } from 'antd';
import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { AssertionType, Entity } from '@src/types.generated';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { ActionsColumn } from '../AcrylAssertionsTableColumns';

import { AssertionName } from './AssertionName';
import { AssertionProfileDrawer } from '../assertion/profile/AssertionProfileDrawer';
import { getAssertionGroupName, getEntityUrnForAssertion, getSiblingWithUrn } from '../acrylUtils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useExpandedRowKeys, useOpenAssertionDetailModal } from '../assertion/builder/hooks';

const StyledTable = styled(Table)`
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
    &&& .ant-table-cell {
        background-color: transparent;
    }

    &&& .acryl-selected-assertions-table-row {
        background-color: ${ANTD_GRAY[4]};
    }

    .group-header {
        cursor: pointer;
        background-color: ${ANTD_GRAY[3]};
    }
    &&& .acryl-assertions-table-row {
        cursor: pointer;
        background-color: ${ANTD_GRAY[2]};
        :hover {
            background-color: ${ANTD_GRAY[3]};
        }
    }
`;
const CategoryType = styled.div`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const LastRun = styled(Typography.Text)`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

export const AssertionListTable = ({
    assertionData,
    filterOptions,
    refetch,
    contract,
    canEditAssertions,
    canEditMonitors,
    canEditSqlAssertions,
}) => {
    const { entityData } = useEntityData();
    const { groupBy } = filterOptions;

    const { expandedRowKeys, setExpandedRowKeys } = useExpandedRowKeys(
        assertionData?.groupBy ? assertionData?.groupBy[groupBy] : [],
    );

    const [focusAssertionUrn, setFocusAssertionUrn] = useState<string | null>(null);
    const focusedAssertion = assertionData.allAssertions.find((assertion) => assertion.urn === focusAssertionUrn);
    const focusedEntityUrn = focusedAssertion ? getEntityUrnForAssertion(focusedAssertion.assertion) : undefined;

    const focusedAssertionEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;

    const canEditFocusAssertion = focusedAssertion
        ? (focusedAssertion?.info?.type === AssertionType.Sql && canEditSqlAssertions) || canEditAssertions
        : false;
    const canEditFocusMonitor = focusedAssertion ? canEditMonitors : false;

    if (focusAssertionUrn && !focusedAssertion) {
        setFocusAssertionUrn(null);
    }
    useOpenAssertionDetailModal(setFocusAssertionUrn);

    const assertionsTableCols: any[] = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (_, record) => <AssertionName record={record} groupBy={groupBy} contract={contract} />,
            width: '50%',
            sorter: (a, b) => a.description?.localeCompare(b.description),
        },
        {
            title: 'Category',
            dataIndex: 'type',
            key: 'type',
            render: (_, record) =>
                !record.groupName && <CategoryType>{getAssertionGroupName(record?.type)}</CategoryType>,
            sorter: (a, b) => a.type?.localeCompare(b.type),
            width: '11%',
        },
        {
            title: 'Last Run',
            dataIndex: 'lastEvaluation',
            key: 'lastEvaluation',
            render: (_, record) => {
                return !record.groupName && <LastRun>{getTimeFromNow(record.lastEvaluationTimeMs)}</LastRun>;
            },
            sorter: (a, b) => (a.lastEvaluationTimeMs || 0) - (b.lastEvaluationTimeMs || 0),
            width: '12%',
        },
        {
            title: 'Tags',
            dataIndex: 'tags',
            key: 'tags',
            width: '12%',
            render: (_, record) => <div>{record.tags}</div>,
        },
        {
            title: '',
            dataIndex: '',
            key: 'actions',
            width: '10%',
            render: (_, record) => {
                const isSqlAssertion = record.type === AssertionType.Sql;
                const assertion = record.assertion;
                return (
                    !record.groupName && (
                        <ActionsColumn
                            assertion={assertion}
                            platform={record.platform}
                            monitor={record.monitor}
                            contract={contract}
                            canEditAssertion={isSqlAssertion ? canEditSqlAssertions : canEditAssertions}
                            canEditMonitor={canEditMonitors}
                            canEditContract
                            lastEvaluationUrl={record.lastEvaluationUrl}
                            refetch={refetch}
                        />
                    )
                );
            },
        },
    ];

    if (groupBy) {
        assertionsTableCols.push({
            title: '',
            key: 'expand',
            width: '2%',
            render: (_, record) => {
                if (record.groupName)
                    return expandedRowKeys.includes(record.name) ? <DownOutlined /> : <RightOutlined />;
            },
        });
    }

    const onAssertionExpand = (_, record) => {
        const key = record.name;
        setExpandedRowKeys((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
    };

    const getGroupData = () => {
        return (assertionData?.groupBy && assertionData?.groupBy[groupBy]) || [];
    };

    const rowClassName = (record) => {
        // return 'row-item';
        if (record.groupName) {
            return 'group-header';
        }
        if (record.urn === focusAssertionUrn) {
            return 'acryl-selected-assertions-table-row' || 'acryl-assertions-table-row';
        } else {
            return 'acryl-assertions-table-row';
        }
    };

    return (
        <>
            <StyledTable
                columns={assertionsTableCols}
                dataSource={groupBy ? getGroupData() : assertionData.allAssertions || []}
                locale={{
                    emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                showHeader
                pagination={false}
                rowClassName={rowClassName}
                rowKey="name"
                expandable={
                    groupBy
                        ? {
                              expandedRowRender: (record: any) => (
                                  <Table
                                      columns={assertionsTableCols}
                                      dataSource={record?.assertions || []}
                                      pagination={false}
                                      showHeader={false}
                                      rowClassName={rowClassName}
                                      onRow={(record: any) => {
                                          return {
                                              onClick: !record.groupName
                                                  ? (_) => {
                                                        setFocusAssertionUrn(record.urn);
                                                    }
                                                  : () => null,
                                          };
                                      }}
                                  />
                              ),
                              onExpand: onAssertionExpand,
                              expandedRowKeys,
                              expandRowByClick: true,
                              expandIcon: () => null,
                          }
                        : undefined
                }
                onRow={(record: any) => {
                    return {
                        onClick: (_) => {
                            setFocusAssertionUrn(record.urn);
                        },
                    };
                }}
            />
            {focusAssertionUrn && focusedAssertionEntity && (
                <AssertionProfileDrawer
                    urn={focusAssertionUrn}
                    entity={focusedAssertionEntity as Entity}
                    contract={contract}
                    canEditAssertion={canEditFocusAssertion}
                    canEditMonitor={canEditFocusMonitor}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                    refetch={refetch}
                />
            )}
        </>
    );
};
