import { Table } from '@components';
import React from 'react';
import styled from 'styled-components';

import { Column } from '@components/components/Table/types';

import { ActionsColumn } from '@app/entityV2/ownership/table/ActionsColumn';

import { OwnershipTypeEntity } from '@types';

const NameText = styled.span`
    font-weight: 700;
`;

type Props = {
    ownershipTypes: OwnershipTypeEntity[];
    setIsOpen: (isOpen: boolean) => void;
    setOwnershipType: (ownershipType: OwnershipTypeEntity) => void;
    refetch: () => void;
};

export const OwnershipTable = ({ ownershipTypes, setIsOpen, setOwnershipType, refetch }: Props) => {
    const columns: Column<OwnershipTypeEntity>[] = [
        {
            title: 'Name',
            key: 'name',
            width: '25%',
            sorter: (a, b) => (a?.info?.name || '').localeCompare(b?.info?.name || ''),
            render: (record) => <NameText>{record?.info?.name || record?.urn}</NameText>,
        },
        {
            title: 'Description',
            key: 'description',
            width: '65%',
            render: (record) => record?.info?.description || '',
        },
        {
            title: '',
            key: 'actions',
            width: '10%',
            alignment: 'right',
            render: (record) => (
                <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                    <ActionsColumn
                        ownershipType={record}
                        setIsOpen={setIsOpen}
                        setOwnershipType={setOwnershipType}
                        refetch={refetch}
                    />
                </div>
            ),
        },
    ];

    return (
        <Table<OwnershipTypeEntity>
            columns={columns}
            data={ownershipTypes}
            rowKey={(record) => record?.info?.name || record.urn}
            isScrollable
            style={{ tableLayout: 'fixed' }}
        />
    );
};
