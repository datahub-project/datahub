import React from 'react';
import { Avatar } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { EntityIconProps } from './types';

export default function UserEntityIcon({ entity }: EntityIconProps) {
    const entityRegistry = useEntityRegistryV2();

    if (!isCorpUser(entity)) return null;

    const imageUrl = entity?.editableProperties?.pictureLink;
    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    return <Avatar name={displayName} imageUrl={imageUrl} size="md" />;
}
