import React, { useState } from 'react';
import { FolderOpenOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { useUserContext } from '../../../context/useUserContext';
import MoveGlossaryEntityModal from './MoveGlossaryEntityModal';
import MoveDomainModal from './MoveDomainModal';
import { useIsNestedDomainsEnabled } from '../../../useAppConfig';
import { isMoveDisabled } from './utils';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityType } from '../../../../types.generated';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { ActionMenuItem } from './styledComponents';

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
