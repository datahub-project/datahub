import React, { useState } from 'react';
import styled from 'styled-components';
import { Table, Typography, Empty } from 'antd';
import { DownOutlined, RightOutlined } from '@ant-design/icons';

import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';
import { IFilter } from './NewAcrylAssertions';
import { ActionsColumn } from '../AcrylAssertionsTableColumns';
import { AssertionType } from '@src/types.generated';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { AssertionResultPopover } from '../assertion/profile/shared/result/AssertionResultPopover';
import { ResultStatusType } from '../assertion/profile/summary/shared/resultMessageUtils';
import { AssertionResultDot } from '../assertion/profile/shared/AssertionResultDot';
import { isMonitorActive } from '../acrylUtils';

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
        background-color: ${ANTD_GRAY[2]};
        :hover {
            background-color: ${ANTD_GRAY[3]};
        }
    }
`;

const StyledAssertionNameContainer = styled.div`
    display: flex;
`;

const StyledDownOutlined = styled(DownOutlined)`
    font-size: 8px;
`;

const StyledRightOutlined = styled(RightOutlined)`
    font-size: 8px;
`;

const Result = styled.div`
    margin: 0px 20px 0px 0px;
    display: flex;
    align-items: center;
`;

const AssertionName = ({ record, groupBy }) => {
    const { primaryLabel } = useBuildAssertionDescriptionLabels(
        groupBy ? record.info : record.assertion.info,
        groupBy ? record.monitor : record.monitor,
    );
    let name = primaryLabel;
    let assertion = record.assertion;

    if (groupBy && record.groupName) {
        name = record.groupName;
    } else if (groupBy && !record.groupName) {
        assertion = record;
    }

    const lastEvaluation = groupBy ? record.runEvents?.runEvents?.[0] : record.lastEvaluation;

    return (
        <div style={{ display: 'flex' }}>
            {!(groupBy && record.groupName) && (
                <AssertionResultPopover
                    assertion={assertion}
                    run={lastEvaluation}
                    showProfileButton
                    placement="right"
                    resultStatusType={ResultStatusType.LATEST}
                >
                    <Result>
                        <AssertionResultDot run={lastEvaluation} size={18} />
                    </Result>
                </AssertionResultPopover>
            )}
            <StyledAssertionNameContainer>{name}</StyledAssertionNameContainer>
        </div>
    );
};

export const AssertionListTable = ({ assertionData, filterOptions, refetch }) => {
    const { groupBy } = filterOptions;
    const [expandedRowKeys, setExpandedRowKeys] = useState<string[]>([]);

    const assertionsTableCols: any[] = [
        {
            title: 'Name',
            dataIndex: 'description',
            key: 'description',
            render: (_, record) => <AssertionName record={record} groupBy={groupBy} />,
            width: '35%',
            sorter: (a, b) => a.description - b.description,
        },
        {
            title: 'Category',
            dataIndex: 'type',
            key: 'type',
            render: (_, record) => <div>{groupBy ? record.info?.type : record.type}</div>,
            sorter: (a, b) => a.type - b.type,
            width: '15%',
        },
        {
            title: 'Last Run',
            dataIndex: 'lastEvaluation',
            key: 'lastEvaluation',
            render: (_, record) => {
                const lastRun = groupBy
                    ? record.runEvents?.runEvents?.[0]?.timestampMillis
                    : record.lastEvaluationTimeMs;
                return !(groupBy && record.groupName) && <Typography.Text>{getTimeFromNow(lastRun)}</Typography.Text>;
            },
            sorter: (a, b) => (a.lastEvaluation?.timestampMillis || 0) - (b.lastEvaluation?.timestampMillis || 0),
            width: '15%',
        },
        {
            title: 'Tags',
            dataIndex: 'tags',
            key: 'tags',
            width: '15%',
            render: (_, record) => <div>{record.tags}</div>,
        },
        {
            title: '',
            dataIndex: '',
            key: 'actions',
            width: '15%',
            render: (_, record) => {
                // const isSqlAssertion = record.type === AssertionType.Sql;
                const assertion = groupBy ? record : record.assertion;
                return (
                    !record.groupName && (
                        <ActionsColumn
                            assertion={assertion}
                            platform={record.platform}
                            monitor={record.monitor}
                            // contract={contract}
                            canEditAssertion={true} //{isSqlAssertion ? canEditSqlAssertions : canEditAssertions}
                            canEditMonitor={true} //{canEditMonitors}
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
            render: (_, record) => {
                if (record.groupName)
                    return expandedRowKeys.includes(record.name) ? (
                        <DownOutlined onClick={() => handleExpand(record.name)} />
                    ) : (
                        <RightOutlined onClick={() => handleExpand(record.name)} />
                    );
            },
        });
    }

    const handleExpand = (key) => {
        setExpandedRowKeys((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
    };

    const getGroupData = () => {
        return (assertionData?.groupBy && assertionData?.groupBy[groupBy]) || [];
    };
    const rowClassName = (record) => {
        if (record.groupName) {
            return 'group-header';
        }
        return '';
    };

    return (
        <StyledTable
            columns={assertionsTableCols}
            dataSource={groupBy ? getGroupData() : assertionData.allAssertions || []}
            rowKey="urn"
            locale={{ emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            showHeader
            pagination={false}
            rowClassName={rowClassName}
            expandable={
                groupBy
                    ? {
                          expandedRowRender: (record: any) => (
                              <Table
                                  columns={assertionsTableCols}
                                  dataSource={record?.assertions || []}
                                  pagination={false}
                                  showHeader={false}
                              />
                          ),
                          expandedRowKeys,
                          expandIcon: () => <></>,
                      }
                    : undefined
            }
        />
    );
};
