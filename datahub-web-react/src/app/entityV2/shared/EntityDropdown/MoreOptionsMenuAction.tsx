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
}

export default function MoreOptionsMenuAction({ menuItems, urn, entityType, entityData, refetch }: Props) {
    return (
        <ActionMenuItem key="view-more">
            <EntityDropdown
                urn={urn}
                entityType={entityType}
                entityData={entityData}
                menuItems={menuItems}
                refetchForEntity={refetch}
            />
        </ActionMenuItem>
    );
}
