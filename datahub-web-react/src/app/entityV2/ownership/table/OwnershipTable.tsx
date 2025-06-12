import { Empty } from 'antd';
import React, { useEffect } from 'react';
import { useInView } from 'react-intersection-observer';
import styled from 'styled-components/macro';

import { ActionsColumn } from '@app/entityV2/ownership/table/ActionsColumn';
import { DescriptionColumn } from '@app/entityV2/ownership/table/DescriptionColumn';
import { NameColumn } from '@app/entityV2/ownership/table/NameColumn';
import { Table } from '@src/alchemy-components';
import { Column } from '@src/alchemy-components/components/Table/types';

import { OwnershipTypeEntity } from '@types';

const TableContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
`;

const LoadMoreIndicator = styled.div`
    height: 20px;
    width: 100%;
`;

type Props = {
    ownershipTypes: OwnershipTypeEntity[];
    setIsOpen: (isOpen: boolean) => void;
    setOwnershipType: (ownershipType: OwnershipTypeEntity) => void;
    refetch: () => void;
    hasMore?: boolean;
    isLoadingMore?: boolean;
    onLoadMore?: () => void;
};

export const OwnershipTable = ({
    ownershipTypes,
    setIsOpen,
    setOwnershipType,
    refetch,
    hasMore = false,
    isLoadingMore = false,
    onLoadMore,
}: Props) => {
    // Using react-intersection-observer to detect when user scrolls to bottom
    const [ref, inView] = useInView({
        threshold: 0,
        triggerOnce: false,
    });

    // Call onLoadMore when the load more indicator comes into view
    useEffect(() => {
        if (inView && hasMore && !isLoadingMore && onLoadMore) {
            onLoadMore();
        }
    }, [inView, hasMore, isLoadingMore, onLoadMore]);

    const tableColumns: Column<OwnershipTypeEntity>[] = [
        {
            title: 'Name',
            key: 'name',
            width: '30%',
            sorter: (a, b) => {
                const nameA = a?.info?.name || '';
                const nameB = b?.info?.name || '';
                return nameA.localeCompare(nameB);
            },
            render: (record) => <NameColumn ownershipType={record} />,
        },
        {
            title: 'Description',
            key: 'description',
            width: '60%',
            render: (record) => <DescriptionColumn ownershipType={record} />,
        },
        {
            title: '',
            key: 'actions',
            alignment: 'right',
            width: '10%',
            render: (record) => (
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

    if (ownershipTypes.length === 0) {
        return <Empty description="No Ownership Types found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    }

    return (
        <TableContainer>
            <Table
                columns={tableColumns}
                data={ownershipTypes}
                rowKey={getRowKey}
                isScrollable
                maxHeight="100%"
                isLoading={isLoadingMore}
            />
            {hasMore && <LoadMoreIndicator ref={ref} />}
        </TableContainer>
    );
};
