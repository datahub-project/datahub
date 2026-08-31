import { Tooltip } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Redirect } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    ActionMenuItem,
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import useDeleteEntity from '@app/entityV2/shared/EntityDropdown/useDeleteEntity';
import { isDeleteDisabled, shouldDisplayChildDeletionWarning } from '@app/entityV2/shared/EntityDropdown/utils';
import { getEntityProfileDeleteRedirectPath } from '@app/shared/deleteUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

interface Props {
    options?: any;
    onDelete?: () => void;
}

export default function DeleteEntityMenuItem({ options, onDelete }: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { urn, entityData, entityType } = useEntityData();
    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
    const isDomainEntity = entityType === EntityType.Domain;

    const { onDeleteEntity, hasBeenDeleted } = useDeleteEntity(
        urn,
        entityType,
        entityData,
        onDelete,
        options?.hideDeleteMessage,
        options?.skipDeleteWait,
    );

    if (!entityData) return null;

    /**
     * A default path to redirect to if the entity is deleted.
     */
    const deleteRedirectPath = getEntityProfileDeleteRedirectPath(entityType, entityData);

    return (
        <Tooltip
            placement="bottom"
            title={(() => {
                const entityName = entityRegistry.getEntityName(entityType);
                if (shouldDisplayChildDeletionWarning(entityType, entityData, me.platformPrivileges)) {
                    return isDomainEntity
                        ? t('delete.cantDeleteSubDomain', { entityName })
                        : t('delete.cantDeleteChild', { entityName });
                }
                return t('delete.tooltip', { entityName });
            })()}
        >
            <ActionMenuItem
                key="delete"
                disabled={isDeleteDisabled(entityType, entityData, me.platformPrivileges)}
                onClick={onDeleteEntity}
                data-testid="entity-menu-delete-button"
            >
                <Trash size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
            </ActionMenuItem>
            {hasBeenDeleted && !onDelete && deleteRedirectPath && <Redirect to={deleteRedirectPath} />}
        </Tooltip>
    );
}
