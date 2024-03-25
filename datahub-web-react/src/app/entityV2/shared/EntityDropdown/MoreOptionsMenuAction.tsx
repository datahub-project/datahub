import React from 'react';
import { ActionMenuItem } from './styledComponents';
import { EntityMenuItems } from './EntityMenuActions';
import EntityDropdown from './EntityDropdown';
import { EntityType } from '../../../../types.generated';

interface Props {
    menuItems: Set<EntityMenuItems>;
    urn: string;
    entityType: EntityType;
    entityData?: any;
    refetch?: () => void;
    size?: number;
    triggerType?: ('click' | 'contextMenu' | 'hover')[] | undefined;
}

export default function MoreOptionsMenuAction({
    menuItems,
    urn,
    entityType,
    entityData,
    refetch,
    size,
    triggerType,
}: Props) {
    return (
        <ActionMenuItem key="view-more" fontSize={size}>
            <EntityDropdown
                urn={urn}
                entityType={entityType}
                entityData={entityData}
                menuItems={menuItems}
                refetchForEntity={refetch}
                triggerType={triggerType}
            />
        </ActionMenuItem>
    );
}
