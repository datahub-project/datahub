import { Button, toast } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Prohibit } from '@phosphor-icons/react/dist/csr/Prohibit';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { Menu } from '@components/components/Menu';
import { ItemType } from '@components/components/Menu/types';

import { UpdateDeprecationModal } from '@app/entityV2/shared/EntityDropdown/UpdateDeprecationModal';
import { useUndeprecateResource } from '@app/entityV2/shared/EntityDropdown/useUndeprecateResource';
import { isDeprecated } from '@app/settingsV2/environments/environments.utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useDeleteCustomFabricTypeMutation } from '@graphql/customFabricType.generated';
import { CustomFabricType } from '@types';

const BUTTON_STYLE = { background: 'none', border: 'none', boxShadow: 'none' };

type Props = {
    environment: CustomFabricType;
    onEdit: (environment: CustomFabricType) => void;
    refetch: () => void;
};

export const EnvironmentActions = ({ environment, onEdit, refetch }: Props) => {
    const { t } = useTranslation('settings.environments');
    const { t: tc } = useTranslation('common.actions');
    const [showDeprecate, setShowDeprecate] = useState(false);
    const [showDelete, setShowDelete] = useState(false);
    const [deleteCustomFabricType] = useDeleteCustomFabricTypeMutation();
    const undeprecate = useUndeprecateResource({ urn: environment.urn, refetch });

    const onDelete = () => {
        deleteCustomFabricType({ variables: { urn: environment.urn } })
            .then(() => {
                toast.success(t('deleteSuccess', { id: environment.id }));
                refetch();
            })
            .catch((e: Error) => {
                // The server refuses while the environment is in use; its message says why.
                toast.error(t('deleteError', { error: e.message }));
            })
            .finally(() => setShowDelete(false));
    };

    const menuItems: ItemType[] = [
        {
            type: 'item',
            key: 'edit',
            title: tc('edit'),
            icon: PencilSimple,
            onClick: () => onEdit(environment),
        },
        {
            type: 'item',
            key: 'copy',
            title: t('menu.copyUrn'),
            icon: Copy,
            onClick: () => navigator.clipboard.writeText(environment.urn),
        },
        isDeprecated(environment)
            ? {
                  type: 'item',
                  key: 'undeprecate',
                  title: t('menu.undeprecate'),
                  icon: Prohibit,
                  onClick: () => {
                      undeprecate();
                  },
              }
            : {
                  type: 'item',
                  key: 'deprecate',
                  title: t('menu.deprecate'),
                  icon: Prohibit,
                  onClick: () => setShowDeprecate(true),
              },
        {
            type: 'item',
            key: 'delete',
            title: tc('delete'),
            icon: Trash,
            onClick: () => setShowDelete(true),
            danger: true,
        },
    ];

    return (
        <>
            <Menu items={menuItems}>
                <Button
                    variant="text"
                    icon={{ icon: DotsThreeVertical, weight: 'bold', size: '2xl', color: 'gray' }}
                    size="lg"
                    isCircle
                    data-testid={`environment-actions-${environment.id}`}
                    style={BUTTON_STYLE}
                />
            </Menu>
            {showDeprecate && (
                <UpdateDeprecationModal
                    urns={[environment.urn]}
                    initialDeprecation={environment.deprecation}
                    onClose={() => setShowDeprecate(false)}
                    refetch={() => refetch()}
                />
            )}
            <ConfirmationModal
                isOpen={showDelete}
                handleConfirm={onDelete}
                handleClose={() => setShowDelete(false)}
                modalTitle={t('deleteTitle', { id: environment.id })}
                modalText={t('deleteText')}
                isDeleteModal
            />
        </>
    );
};
