import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityMenuActions } from '@app/entityV2/Entity';
import EntityDropdown from '@app/entityV2/shared/EntityDropdown/EntityDropdown';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { DeprecationFormData } from '@app/entityV2/shared/EntityDropdown/useHandleDeprecateDomain';

import { EntityType } from '@types';

interface Props {
    menuItems: Set<EntityMenuItems>;
    urn: string;
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    refetch?: () => void;
    size?: number;
    triggerType?: ('click' | 'contextMenu' | 'hover')[] | undefined;
    actions?: EntityMenuActions;
    refetchDeprecation?: (formData?: DeprecationFormData) => void;
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
    refetchDeprecation,
}: Props) {
    return (
        <ActionMenuItem key="view-more" fontSize={size} data-testid="view-more-button">
            <EntityDropdown
                urn={urn}
                entityType={entityType}
                entityData={entityData}
                menuItems={menuItems}
                refetchForEntity={refetch}
                triggerType={triggerType}
                onEditEntity={actions?.onEdit}
                onDeleteEntity={actions?.onDelete}
                refetchDeprecation={refetchDeprecation}
            />
        </ActionMenuItem>
    );
}
