import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { ActionMenuItem } from './styledComponents';
import { EntityMenuItems } from './EntityMenuActions';
import EntityDropdown from './EntityDropdown';
import { EntityType } from '../../../../types.generated';
import { EntityMenuActions } from '../../Entity';

interface Props {
    menuItems: Set<EntityMenuItems>;
    urn: string;
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    refetch?: () => void;
    size?: number;
    triggerType?: ('click' | 'contextMenu' | 'hover')[] | undefined;
    actions?: EntityMenuActions;
}

export default function MoreOptionsMenuAction({
    menuItems,
    urn,
    entityType,
    entityData,
    refetch,
    size,
    triggerType,
    actions,
}: Props) {
    return (
        <ActionMenuItem key="view-more" fontSize={size} excludeMargin>
            <EntityDropdown
                urn={urn}
                entityType={entityType}
                entityData={entityData}
                menuItems={menuItems}
                refetchForEntity={refetch}
                triggerType={triggerType}
                onEditEntity={actions?.onEdit}
                onDeleteEntity={actions?.onDelete}
            />
        </ActionMenuItem>
    );
}
