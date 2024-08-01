import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { Empty, Table } from 'antd';
import styled from 'styled-components';
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';
import { IFilter } from './NewAcrylAssertions';
import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { ActionsColumn } from '../AcrylAssertionsTableColumns';
import { AssertionType } from '@src/types.generated';

export const StyledTable = styled(Table)`
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

const StyledAssertionNameContainer = styled.div`
    display: flex;
`;

const StyledDownOutlined = styled(DownOutlined)`
    font-size: 8px;
`;

const StyledRightOutlined = styled(RightOutlined)`
    font-size: 8px;
`;

// type AssertionTableType = {
//     dataSource: any[];
// };

export const AssertionListTable = ({
    assertionData,
    filterOptions,
    refetch,
}: {
    assertionData: any;
    filterOptions: IFilter;
    refetch: () => void;
}) => {
    const { groupBy } = filterOptions;
    const AssertionName = ({ record }: any) => {
        const { primaryLabel } = useBuildAssertionDescriptionLabels(
            groupBy ? record.info : record.assertion.info,
            groupBy ? record.monitor : record.monitor,
        );
        let name = primaryLabel;
        if (groupBy && record.groupName) {
            name = record.groupName;
        }

        return <StyledAssertionNameContainer>{name}</StyledAssertionNameContainer>;
    };

    const assertionsTableCols = [
        {
            title: 'Name',
            dataIndex: 'description',
            key: 'description',
            render: (_, record: any) => {
                return <AssertionName record={record} />;
            },
            width: '35%',
            sorter: (a, b) => a.description - b.description,
        },
        {
            title: 'Category',
            dataIndex: 'type',
            key: 'type',
            render: (_, record: any) => {
                return <div>{groupBy ? record.info?.type : record.type}</div>;
            },
            sorter: (a, b) => {
                return a.type - b.type;
            },
            width: '15%',
        },
        {
            title: 'Last Run',
            dataIndex: 'lastEvaluation',
            key: 'type',
            render: (_, record) => {
                return <div>{record.lastEvaluation?.status || 'N/A'}</div>;
            },
            sorter: (a, b) => (a.lastEvaluation?.timestampMillis || 0) - (b.lastEvaluation?.timestampMillis || 0),
            width: '15%',
        },
        {
            title: 'Tags',
            dataIndex: '',
            key: 'tags',
            width: '15%',
            render: (_, record?: any) => {
                return <div> {record.tags} </div>;
            },
        },
        {
            title: '',
            dataIndex: '',
            key: '',
            width: '15%',
            render: (_, record?: any) => {
                const isSqlAssertion = record.type === AssertionType.Sql;
                console.log('record>>>>', record);
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

    const getGroupData = () => {
        return (assertionData?.groupBy && assertionData?.groupBy[groupBy]) || [];
    };
    return (
        <StyledTable
            columns={assertionsTableCols}
            dataSource={groupBy ? getGroupData() : assertionData.allAssertions || []}
            rowKey="urn"
            locale={{
                emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            showHeader={true}
            pagination={false}
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
                          expandIcon: ({ expanded, onExpand, record }: any) =>
                              expanded ? (
                                  <StyledDownOutlined onClick={(e) => onExpand(record, e)} />
                              ) : (
                                  <StyledRightOutlined onClick={(e) => onExpand(record, e)} />
                              ),
                      }
                    : undefined
            }
        />
    );
};
