import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { AvatarType } from '@components/components/AvatarStack/types';
import { AvatarSizeOptions } from '@components/theme/config';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';

import { CorpGroup, CorpUser, EntityType } from '@types';

function resolveEntityType(user: CorpUser | CorpGroup): EntityType {
    if (user.type) return user.type;
    return user.__typename === 'CorpGroup' ? EntityType.CorpGroup : EntityType.CorpUser;
}

interface Props {
    user?: CorpUser | CorpGroup;
    size?: AvatarSizeOptions;
    entityRegistry: EntityRegistry;
}

export default function AvatarPillWithLinkAndHover({ user, size, entityRegistry }: Props) {
    if (!user) return null;

    const entityType = resolveEntityType(user);
    const avatarUrl = user.editableProperties?.pictureLink;

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
                    type={entityType === EntityType.CorpUser ? AvatarType.user : AvatarType.group}
                    showInPill
                />
            </Link>
        </HoverEntityTooltip>
    );
}
