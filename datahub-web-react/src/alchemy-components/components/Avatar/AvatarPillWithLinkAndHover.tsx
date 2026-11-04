import React from 'react';
import { Link } from 'react-router-dom';

import { Avatar } from '@components/components/Avatar';
import { AvatarType } from '@components/components/AvatarStack/types';
import { AvatarSizeOptions } from '@components/theme/config';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { ActorEntity, getActorPictureLink } from '@app/entityV2/shared/utils/actorUtils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';

import { EntityType } from '@types';

function resolveEntityType(user: ActorEntity): EntityType {
    return user.type;
}

interface Props {
    user?: ActorEntity;
    size?: AvatarSizeOptions;
    entityRegistry: EntityRegistry;
}

export default function AvatarPillWithLinkAndHover({ user, size, entityRegistry }: Props) {
    if (!user) return null;

    const entityType = resolveEntityType(user);
    const avatarUrl = getActorPictureLink(user);

    return (
        <HoverEntityTooltip entity={user} showArrow={false}>
            <Link
                to={`${entityRegistry.getEntityUrl(entityType, user.urn)}`}
                onClick={(e) => {
                    e.stopPropagation();
                }}
            >
                <Avatar
                    name={entityRegistry.getDisplayName(entityType, user)}
                    imageUrl={avatarUrl}
                    size={size}
                    type={entityType === EntityType.CorpGroup ? AvatarType.group : AvatarType.user}
                    showInPill
                />
            </Link>
        </HoverEntityTooltip>
    );
}
