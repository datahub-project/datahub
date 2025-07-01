import { FolderOpenOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React, { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import MoveDomainModal from '@app/entityV2/shared/EntityDropdown/MoveDomainModal';
import MoveGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/MoveGlossaryEntityModal';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { isMoveDisabled } from '@app/entityV2/shared/EntityDropdown/utils';
import { useIsNestedDomainsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export default function MoveEntityMenuAction() {
    const { entityData, entityType, urn } = useEntityData();
    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const [isMoveModalVisible, setIsMoveModalVisible] = useState(false);
    const isGlossaryEntity = entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm;
    const isDomainEntity = entityType === EntityType.Domain;
    const isDomainMoveHidden = !isNestedDomainsEnabled && isDomainEntity;

    if (isDomainMoveHidden) {
        return null;
    }

    return (
        <Tooltip placement="bottom" title={`Move this ${entityRegistry.getEntityName(entityType)}`} showArrow={false}>
            <ActionMenuItem
                key="move"
                disabled={isMoveDisabled(entityType, entityData, me.platformPrivileges)}
                onClick={() => setIsMoveModalVisible(true)}
                data-testid="entity-menu-move-button"
            >
                <FolderOpenOutlined style={{ display: 'flex' }} />
            </ActionMenuItem>
            {isMoveModalVisible && isGlossaryEntity && (
                <MoveGlossaryEntityModal
                    entityData={entityData}
                    entityType={entityType}
                    urn={urn}
                    onClose={() => setIsMoveModalVisible(false)}
                />
            )}
            {isMoveModalVisible && isDomainEntity && <MoveDomainModal onClose={() => setIsMoveModalVisible(false)} />}
        </Tooltip>
    );
}
