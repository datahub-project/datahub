import { Menu, toast } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { ItemType } from '@components/components/Menu/types';

import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useDeleteDomainMutation } from '@graphql/domain.generated';
import { EntityType } from '@types';

type Props = {
    urn: string;
    name: string;
    onDelete?: () => void;
};

export default function DomainItemMenu({ name, urn, onDelete }: Props) {
    const { t } = useTranslation('governance.domain');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const [deleteDomainMutation] = useDeleteDomainMutation();
    const [showDeleteModal, setShowDeleteModal] = useState(false);

    const deleteDomain = () => {
        deleteDomainMutation({
            variables: {
                urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('itemMenu.deleteSuccess'));
                    onDelete?.();
                }
            })
            .catch((e) => {
                console.error('Issue deleting a domain:', e);
                toast.destroy();
                toast.error(t('itemMenu.deleteError'), { duration: 3 });
            });
    };

    const handleDelete = () => {
        setShowDeleteModal(false);
        deleteDomain();
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
                modalTitle={t('itemMenu.deleteConfirmTitle', { name })}
                modalText={t('itemMenu.deleteConfirmText', {
                    entityName: entityRegistry.getEntityName(EntityType.Domain),
                })}
                confirmButtonText={tc('delete')}
                isDeleteModal
            />
        </>
    );
}
