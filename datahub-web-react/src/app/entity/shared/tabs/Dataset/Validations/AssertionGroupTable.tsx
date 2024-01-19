import React, { useState } from 'react';
import styled from 'styled-components';
import { Empty } from 'antd';
import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { Assertion, DataContract } from '../../../../../../types.generated';
import { AssertionGroup } from './acrylTypes';
import { AssertionGroupHeader } from './AssertionGroupHeader';
import { AcrylDatasetAssertionsList } from './AcrylAssertionsList';
import { StyledTable } from '../../../components/styled/StyledTable';
import { useExpandRowBasedOnAssertionUrn } from './assertion/builder/hooks';

const StyledStyledTable = styled(StyledTable)`
    &&&& {
        .ant-table-cell {
            padding-left: 0px;
        }
        .ant-table-row-expand-icon-cell {
            padding: 16px;
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
    groups: AssertionGroup[];
    contract?: DataContract;
    onDeletedAssertion: (urn: string) => void;
    onUpdatedAssertion: (assertion: Assertion) => void;
};

export const AssertionGroupTable = ({ groups, contract, onDeletedAssertion, onUpdatedAssertion }: Props) => {
    const [expandedRowKeys, setExpandedRowKeys] = useState<string[] | undefined>(undefined);

    // To handle assertion URN parameter logic for expanding rows
    useExpandRowBasedOnAssertionUrn(groups, setExpandedRowKeys);

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (_, record: any) => <AssertionGroupHeader group={record} />,
        },
    ];

    return (
        <StyledStyledTable
            columns={columns}
            dataSource={groups}
            rowKey="name"
            showHeader={false}
            locale={{
                emptyText: <Empty description="No Assertions Found" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            expandable={{
                expandedRowKeys,
                expandedRowRender: (group, _index, _indent, _expanded) => {
                    return (
                        <AcrylDatasetAssertionsList
                            assertions={group.assertions}
                            contract={contract}
                            onDeletedAssertion={onDeletedAssertion}
                            onUpdatedAssertion={onUpdatedAssertion}
                            setExpandedRowKeys={setExpandedRowKeys}
                        />
                    );
                },
                expandIcon: ({ expanded, onExpand, record }: any) =>
                    expanded ? (
                        <StyledDownOutlined onClick={(e) => onExpand(record, e)} />
                    ) : (
                        <StyledRightOutlined onClick={(e) => onExpand(record, e)} />
                    ),
                expandRowByClick: true,
                defaultExpandAllRows: true,
            }}
            pagination={false}
        />
    );
};
