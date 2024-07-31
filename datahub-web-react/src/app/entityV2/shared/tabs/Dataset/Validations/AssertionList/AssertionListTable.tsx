import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { Empty, Table } from 'antd';
import styled from 'styled-components';
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';
import { IFilter } from './NewAcrylAssertions';

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

// type AssertionTableType = {
//     dataSource: any[];
// };

export const AssertionListTable = ({
    assertionData,
    filterOptions,
}: {
    assertionData: any;
    filterOptions: IFilter;
}) => {
    const { groupBy } = filterOptions;
    const AssertionName = ({ record }: any) => {
        const { primaryLabel } = useBuildAssertionDescriptionLabels(
            groupBy ? record.info : record.assertion.info,
            groupBy ? record.monitor : record.monitor,
        );
        return <StyledAssertionNameContainer>{primaryLabel}</StyledAssertionNameContainer>;
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
                return <div>{record.type}</div>;
            },
            sorter: (a, b) => {
                return a.type - b.type;
            },
            width: '15%',
        },
        {
            title: 'Last Run',
            dataIndex: 'type',
            key: 'type',
            render: (_, record: any) => {
                return <div>{record.type}</div>;
            },
            sorter: (a, b) => {
                return a.type - b.type;
            },
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
            render: () => {
                return <div> Actions </div>;
            },
        },
    ];

    const groupedColumns = [
        {
            title: 'Type',
            dataIndex: '',
            key: 'type',
            render: (text, record) => (
                <span>
                    {record.type}
                    <Table
                        columns={assertionsTableCols}
                        dataSource={record?.assertions || []}
                        pagination={false}
                        rowKey="key"
                        // onChange={handleChange}
                    />
                </span>
            ),
        },
    ];
    const getGroupData = () => {
        return (assertionData?.groupBy && assertionData?.groupBy[groupBy]) || [];
    };
    return (
        <StyledTable
            // columns={assertionsTableCols}
            // dataSource={dataSource || []}
            columns={groupBy ? groupedColumns : assertionsTableCols}
            dataSource={groupBy ? getGroupData() : assertionData.allAssertions || []}
            rowKey="urn"
            locale={{
                emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            showHeader={true}
            pagination={false}
        />
    );
};
