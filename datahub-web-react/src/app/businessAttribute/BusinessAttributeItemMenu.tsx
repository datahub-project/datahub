import { Menu, toast } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { ItemType } from '@components/components/Menu/types';

import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useDeleteBusinessAttributeMutation } from '@graphql/businessAttribute.generated';

type Props = {
    urn: string;
    title: string | undefined;
    onDelete?: () => void;
};

export default function BusinessAttributeItemMenu({ title, urn, onDelete }: Props) {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const [deleteBusinessAttributeMutation] = useDeleteBusinessAttributeMutation();
    const [showDeleteModal, setShowDeleteModal] = useState(false);

    const deleteBusinessAttribute = () => {
        deleteBusinessAttributeMutation({
            variables: {
                urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('businessAttribute.deleteSuccess'));
                    onDelete?.();
                }
            })
            .catch(() => {
                toast.destroy();
                toast.error(t('businessAttribute.deleteError'), { duration: 3 });
            });
    };

    const handleDelete = () => {
        setShowDeleteModal(false);
        deleteBusinessAttribute();
    };

    const items: ItemType[] = [
        {
            type: 'item',
            key: 'delete',
            title: tc('delete'),
            icon: Trash,
            danger: true,
            onClick: () => setShowDeleteModal(true),
        },
    ];

    return (
        <>
            <Menu items={items} trigger={['click']}>
                <MenuIcon data-testid={`dropdown-menu-${urn}`} fontSize={20} />
            </Menu>
            <ConfirmationModal
                isOpen={showDeleteModal}
                handleClose={() => setShowDeleteModal(false)}
                handleConfirm={handleDelete}
                modalTitle={t('businessAttribute.deleteModalTitle', { title })}
                modalText={t('businessAttribute.deleteConfirmation')}
                confirmButtonText={tc('delete')}
                isDeleteModal
            />
        </>
    );
}
