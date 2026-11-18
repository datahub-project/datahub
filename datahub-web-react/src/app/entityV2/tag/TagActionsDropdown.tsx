import React from 'react';

import EntityDropdown from '@app/entityV2/shared/EntityDropdown/EntityDropdown';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { GenericEntityProperties } from '@app/entityV2/shared/types';

import { EntityType } from '@types';

const TAG_MENU_ITEMS = new Set([EntityMenuItems.UPDATE_DEPRECATION, EntityMenuItems.DELETE]);

type Props = {
    urn: string;
    tagData: GenericEntityProperties | null;
};

export default function TagActionsDropdown({ urn, tagData }: Props) {
    return <EntityDropdown urn={urn} entityType={EntityType.Tag} entityData={tagData} menuItems={TAG_MENU_ITEMS} />;
}
