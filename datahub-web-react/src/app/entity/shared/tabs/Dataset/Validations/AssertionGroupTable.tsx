import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { Empty } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { StyledTable } from '@app/entity/shared/components/styled/StyledTable';
import { AcrylDatasetAssertionsList } from '@app/entity/shared/tabs/Dataset/Validations/AcrylAssertionsList';
import { AssertionGroupHeader } from '@app/entity/shared/tabs/Dataset/Validations/AssertionGroupHeader';
import { AssertionGroup } from '@app/entity/shared/tabs/Dataset/Validations/acrylTypes';
import { useExpandedRowKeys } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/hooks';

import { DataContract } from '@types';

const StyledStyledTable = styled(StyledTable)`
    &&&& {
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
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
    refetch: () => void;
};

export const AssertionGroupTable = ({
    groups,
    contract,
    canEditAssertions,
    canEditMonitors,
    canEditSqlAssertions,
    refetch,
}: Props) => {
    const { expandedRowKeys, setExpandedRowKeys } = useExpandedRowKeys(groups);

    const onAssertionExpand = (expanded, record) => {
        if (expanded) {
            setExpandedRowKeys((prevKeys) => [...prevKeys, record.name]);
        } else {
            setExpandedRowKeys((prevKeys) => prevKeys.filter((key) => key !== record.name));
        }
    };

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
                onExpand: onAssertionExpand,
                expandedRowRender: (group, _index, _indent, _expanded) => {
                    return (
                        <AcrylDatasetAssertionsList
                            assertions={group.assertions}
                            contract={contract}
                            canEditAssertions={canEditAssertions}
                            canEditMonitors={canEditMonitors}
                            canEditSqlAssertions={canEditSqlAssertions}
                            refetch={refetch}
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
            }}
            pagination={false}
        />
    );
};
