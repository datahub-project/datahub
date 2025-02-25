import React from 'react';
import { Avatar } from '@src/alchemy-components';
import useEntityUtils from '@src/app/entityV2/shared/hooks/useEntityUtils';
import { CorpUser, EntityType } from '@src/types.generated';
import { EntityIconProps } from './types';

export default function UserEntityIcon({ entity }: EntityIconProps) {
    const { getUserPictureLink, getDisplayName } = useEntityUtils();

    if (entity.type !== EntityType.CorpUser) return null;

    const imageUrl = getUserPictureLink(entity as CorpUser);
    const displayName = getDisplayName(entity);

    return <Avatar name={displayName} imageUrl={imageUrl} size="md" />;
}
