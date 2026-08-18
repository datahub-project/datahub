import { Tooltip } from '@components';
import { FolderOpen } from '@phosphor-icons/react/dist/csr/FolderOpen';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import MoveDataProductModal from '@app/entityV2/shared/EntityDropdown/MoveDataProductModal';
import MoveDomainModal from '@app/entityV2/shared/EntityDropdown/MoveDomainModal';
import MoveGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/MoveGlossaryEntityModal';
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

    const closeMoveModal = () => setIsMoveModalVisible(false);

    function renderMoveModal() {
        if (!isMoveModalVisible) return null;

        switch (entityType) {
            case EntityType.GlossaryNode:
            case EntityType.GlossaryTerm:
                return (
                    <MoveGlossaryEntityModal
                        entityData={entityData}
                        entityType={entityType}
                        urn={urn}
                        onClose={closeMoveModal}
                    />
                );
            case EntityType.Domain:
                return <MoveDomainModal onClose={closeMoveModal} />;
            case EntityType.DataProduct:
                return <MoveDataProductModal onClose={closeMoveModal} />;
            default:
                return null;
        }
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
            {renderMoveModal()}
        </Tooltip>
    );
}
