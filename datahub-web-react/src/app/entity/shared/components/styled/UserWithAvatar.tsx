import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { AvatarSizeOptions } from '@components/theme/config';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser } from '@types';

interface Props {
    user?: CorpUser;
    size?: AvatarSizeOptions;
}

export default function UserWithAvatar({ user, size }: Props) {
    const entityRegistry = useEntityRegistry();

    if (!user) return null;

    const avatarUrl = user.editableProperties?.pictureLink;

    return (
        <HoverEntityTooltip entity={user} showArrow={false}>
            <Link to={`${entityRegistry.getEntityUrl(user.type, user.urn)}`}>
                <Avatar
                    name={entityRegistry.getDisplayName(user.type, user)}
                    imageUrl={avatarUrl}
                    size={size}
                    showInPill
                />
            </Link>
        </HoverEntityTooltip>
    );
}
