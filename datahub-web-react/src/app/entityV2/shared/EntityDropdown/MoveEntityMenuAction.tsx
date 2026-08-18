import { Tooltip } from '@components';
import { FolderOpen } from '@phosphor-icons/react/dist/csr/FolderOpen';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import MoveEntityModal from '@app/entityV2/shared/EntityDropdown/MoveEntityModal';
import {
    ActionMenuItem,
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { isMoveDisabled } from '@app/entityV2/shared/EntityDropdown/utils';
import { useIsNestedDomainsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export default function MoveEntityMenuAction() {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { entityData, entityType, urn } = useEntityData();
    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const [isMoveModalVisible, setIsMoveModalVisible] = useState(false);
    const isDomainMoveHidden = !isNestedDomainsEnabled && entityType === EntityType.Domain;

    if (isDomainMoveHidden) {
        return null;
    }

    return (
        <Tooltip
            placement="bottom"
            title={t('menuAction.moveTooltip', { entityName: entityRegistry.getEntityName(entityType) })}
            showArrow={false}
        >
            <ActionMenuItem
                key="move"
                disabled={isMoveDisabled(entityType, entityData, me.platformPrivileges)}
                onClick={() => setIsMoveModalVisible(true)}
                data-testid="entity-menu-move-button"
            >
                <FolderOpen size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
            </ActionMenuItem>
            {isMoveModalVisible && (
                <MoveEntityModal
                    entityType={entityType}
                    entityData={entityData}
                    urn={urn}
                    onClose={() => setIsMoveModalVisible(false)}
                />
            )}
        </Tooltip>
    );
}
