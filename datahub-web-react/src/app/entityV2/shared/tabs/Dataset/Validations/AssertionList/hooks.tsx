import { useMemo } from 'react';
import styled from 'styled-components';
import { AssertionName } from './AssertionName';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Typography } from 'antd';
import { getAssertionGroupName } from '../acrylUtils';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { AssertionType } from '@src/types.generated';
import { ActionsColumn } from '../AcrylAssertionsTableColumns';
import { DownOutlined, RightOutlined } from '@ant-design/icons';

const CategoryType = styled.div`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const LastRun = styled(Typography.Text)`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

export const useAssertionsTableColumns = ({
    groupBy,
    contract,
    canEditSqlAssertions,
    canEditAssertions,
    canEditMonitors,
    refetch,
    expandedRowKeys,
}) => {
    return useMemo(() => {
        const columns = [
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
                render: (_, record) => <div>{record.tags?.name}</div>,
            },
            {
                title: '',
                dataIndex: '',
                key: 'actions',
                width: '10%',
                render: (_, record) => {
                    const isSqlAssertion = record.type === AssertionType.Sql;
                    const { assertion } = record;
                    return (
                        !record.groupName && (
                            <ActionsColumn
                                assertion={assertion}
                                monitor={record.monitor}
                                contract={contract}
                                canEditAssertion={isSqlAssertion ? canEditSqlAssertions : canEditAssertions}
                                canEditMonitor={canEditMonitors}
                                canEditContract
                                refetch={refetch}
                            />
                        )
                    );
                },
            },
        ];

        if (groupBy) {
            columns.push({
                title: '',
                key: 'expand',
                dataIndex: '',
                width: '2%',
                render: (_, record) => {
                    return (
                        record.groupName &&
                        (expandedRowKeys.includes(record.name) ? <DownOutlined /> : <RightOutlined />)
                    );
                },
            });
        }

        return columns;
    }, [groupBy, contract, canEditSqlAssertions, canEditAssertions, canEditMonitors, refetch, expandedRowKeys]);
};
