import { Button, toast } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { Menu } from '@components/components/Menu';
import { ItemType } from '@components/components/Menu/types';

import { useDeleteOwnershipTypeMutation } from '@graphql/ownership.generated';
import { OwnershipTypeEntity } from '@types';

const BUTTON_STYLE = { background: 'none', border: 'none', boxShadow: 'none' };

type Props = {
    ownershipType: OwnershipTypeEntity;
    setIsOpen: (isOpen: boolean) => void;
    setOwnershipType: (ownershipType: OwnershipTypeEntity) => void;
    refetch: () => void;
};

export const ActionsColumn = ({ ownershipType, setIsOpen, setOwnershipType, refetch }: Props) => {
    const { t } = useTranslation('entity.ownership');
    const { t: tc } = useTranslation('common.actions');
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
                toast.success(t('deleteSuccess'));
                setTimeout(() => {
                    refetch();
                }, 3000);
            })
            .catch(() => {
                toast.error(t('deleteError'));
            });
    };

    const menuItems: ItemType[] = [
        {
            type: 'item',
            key: 'edit',
            title: tc('edit'),
            icon: PencilSimple,
            onClick: onEdit,
        },
        {
            type: 'item',
            key: 'copy',
            title: t('menu.copyUrn'),
            icon: Copy,
            onClick: onCopy,
        },
        {
            type: 'item',
            key: 'delete',
            title: tc('delete'),
            icon: Trash,
            onClick: onDelete,
            danger: true,
        },
    ];

    return (
        <Menu items={menuItems}>
            <Button
                variant="text"
                icon={{ icon: DotsThreeVertical, weight: 'bold', size: '2xl', color: 'gray' }}
                size="lg"
                isCircle
                data-testid="ownership-table-dropdown"
                style={BUTTON_STYLE}
            />
        </Menu>
    );
};
