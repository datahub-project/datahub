import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import handleGraphQLError from '@app/shared/handleGraphQLError';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Button, Menu, toast } from '@src/alchemy-components';

import { useDeletePostMutation } from '@graphql/post.generated';

type Props = {
    urn: string;
    title: string;
    onDelete?: () => void;
    onEdit?: () => void;
};

export default function PostItemMenu({ title, urn, onDelete, onEdit }: Props) {
    const { t } = useTranslation('settings.posts');
    const { t: tc } = useTranslation('common.actions');
    const [deletePostMutation] = useDeletePostMutation();
    const [showConfirmDelete, setShowConfirmDelete] = useState(false);

    const deletePost = () => {
        deletePostMutation({
            variables: {
                urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('deleteSuccess'));
                    onDelete?.();
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: t('deleteError'),
                    permissionMessage: t('deleteUnauthorized'),
                });
            });
    };

    return (
        <>
            <Menu
                items={[
                    {
                        type: 'item',
                        key: 'edit',
                        title: tc('edit'),
                        icon: PencilSimple,
                        onClick: () => onEdit?.(),
                    },
                    {
                        type: 'item',
                        key: 'delete',
                        title: tc('delete'),
                        icon: Trash,
                        danger: true,
                        onClick: () => setShowConfirmDelete(true),
                    },
                ]}
            >
                <Button
                    variant="text"
                    isCircle
                    size="lg"
                    icon={{ icon: DotsThreeVertical, size: 'xl', weight: 'bold' }}
                    data-testid="dropdown-menu-item"
                />
            </Menu>
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={() => setShowConfirmDelete(false)}
                handleConfirm={deletePost}
                modalTitle={t('deleteConfirmTitle', { title })}
                modalText={t('deleteConfirmText')}
            />
        </>
    );
}
