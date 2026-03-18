import { Button, toast } from '@components';
import React from 'react';

import { Menu } from '@components/components/Menu';
import { ItemType } from '@components/components/Menu/types';

import { useDeleteOwnershipTypeMutation } from '@graphql/ownership.generated';
import { OwnershipTypeEntity } from '@types';

type Props = {
    ownershipType: OwnershipTypeEntity;
    setIsOpen: (isOpen: boolean) => void;
    setOwnershipType: (ownershipType: OwnershipTypeEntity) => void;
    refetch: () => void;
};

export const ActionsColumn = ({ ownershipType, setIsOpen, setOwnershipType, refetch }: Props) => {
    const [deleteOwnershipTypeMutation] = useDeleteOwnershipTypeMutation();

    const onEdit = () => {
        setIsOpen(true);
        setOwnershipType(ownershipType);
    };

    const onCopy = () => {
        navigator.clipboard.writeText(ownershipType.urn);
    };

    const onDelete = () => {
        deleteOwnershipTypeMutation({
            variables: {
                urn: ownershipType.urn,
            },
        })
            .then(() => {
                toast.success('Successfully deleted ownership type.');
                setTimeout(() => {
                    refetch();
                }, 3000);
            })
            .catch(() => {
                toast.error('Failed to delete ownership type');
            });
    };

    const menuItems: ItemType[] = [
        {
            type: 'item',
            key: 'edit',
            title: 'Edit',
            icon: 'PencilSimple',
            onClick: onEdit,
        },
        {
            type: 'item',
            key: 'copy',
            title: 'Copy Urn',
            icon: 'Copy',
            onClick: onCopy,
        },
        {
            type: 'item',
            key: 'delete',
            title: 'Delete',
            icon: 'Trash',
            onClick: onDelete,
            danger: true,
        },
    ];

    return (
        <Menu items={menuItems}>
            <Button
                variant="text"
                icon={{ icon: 'DotsThreeVertical', weight: 'bold', size: '2xl', source: 'phosphor', color: 'gray' }}
                size="lg"
                isCircle
                data-testid="ownership-table-dropdown"
                style={{ background: 'none', border: 'none', boxShadow: 'none' }}
            />
        </Menu>
    );
};
