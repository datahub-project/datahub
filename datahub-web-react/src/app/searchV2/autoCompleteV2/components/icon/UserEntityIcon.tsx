import React from 'react';

import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import { Avatar } from '@src/alchemy-components';
import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';
import { isCorpUser } from '@src/app/entityV2/user/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

// Avatar only exposes a fixed set of size tokens (smallest is 18px), so a caller-provided
// px `size` picks the nearest token rather than rendering at an exact size.
function getAvatarSizeOption(size?: number): AvatarSizeOptions {
    if (size === undefined) return 'md';
    if (size <= 18) return 'sm';
    if (size <= 24) return 'md';
    if (size <= 28) return 'lg';
    if (size <= 32) return 'xl';
    return '2xl';
}

export default function UserEntityIcon({ entity, size }: EntityIconProps) {
    const entityRegistry = useEntityRegistryV2();

    if (!isCorpUser(entity)) return null;

    const imageUrl = entity?.editableProperties?.pictureLink;
    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    return <Avatar name={displayName} imageUrl={imageUrl} size={getAvatarSizeOption(size)} />;
}
