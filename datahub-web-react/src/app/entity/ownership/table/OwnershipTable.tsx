import { Empty } from 'antd';
import React from 'react';

import { ActionsColumn } from '@app/entity/ownership/table/ActionsColumn';
import { DescriptionColumn } from '@app/entity/ownership/table/DescriptionColumn';
import { NameColumn } from '@app/entity/ownership/table/NameColumn';
import { StyledTable } from '@app/entity/shared/components/styled/StyledTable';

import { OwnershipTypeEntity } from '@types';

type Props = {
    ownershipTypes: OwnershipTypeEntity[];
    setIsOpen: (isOpen: boolean) => void;
    setOwnershipType: (ownershipType: OwnershipTypeEntity) => void;
    refetch: () => void;
};

export const OwnershipTable = ({ ownershipTypes, setIsOpen, setOwnershipType, refetch }: Props) => {
    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            sorter: (a: any, b: any) => a?.info?.name?.localeCompare(b?.info?.name),
            key: 'name',
            render: (_, record: any) => <NameColumn ownershipType={record} />,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (_, record: any) => <DescriptionColumn ownershipType={record} />,
        },
        {
            dataIndex: 'actions',
            key: 'actions',
            render: (_, record: any) => (
                <ActionsColumn
                    ownershipType={record}
                    setIsOpen={setIsOpen}
                    setOwnershipType={setOwnershipType}
                    refetch={refetch}
                />
            ),
        },
    ];

    const getRowKey = (ownershipType: OwnershipTypeEntity) => {
        return ownershipType?.info?.name || ownershipType.urn;
    };

    return (
        <StyledTable
            columns={tableColumns}
            dataSource={ownershipTypes}
            rowKey={getRowKey}
            locale={{
                emptyText: <Empty description="No Ownership Types found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            pagination={false}
        />
    );
};
