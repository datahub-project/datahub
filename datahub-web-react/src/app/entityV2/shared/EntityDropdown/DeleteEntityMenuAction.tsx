import { DeleteOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React from 'react';
import { Redirect } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
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
            title={
                shouldDisplayChildDeletionWarning(entityType, entityData, me.platformPrivileges)
                    ? `Can't delete ${entityRegistry.getEntityName(entityType)} with ${
                          isDomainEntity ? 'sub-domain' : 'child'
                      } entities.`
                    : `Delete this ${entityRegistry.getEntityName(entityType)}`
            }
        >
            <ActionMenuItem
                key="delete"
                disabled={isDeleteDisabled(entityType, entityData, me.platformPrivileges)}
                onClick={onDeleteEntity}
                data-testid="entity-menu-delete-button"
            >
                <DeleteOutlined style={{ display: 'flex' }} />
            </ActionMenuItem>
            {hasBeenDeleted && !onDelete && deleteRedirectPath && <Redirect to={deleteRedirectPath} />}
        </Tooltip>
    );
}
