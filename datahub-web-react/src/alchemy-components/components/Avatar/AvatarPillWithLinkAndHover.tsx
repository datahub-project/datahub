/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { AvatarType } from '@components/components/AvatarStack/types';
import { AvatarSizeOptions } from '@components/theme/config';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';

import { CorpGroup, CorpUser, EntityType } from '@types';

interface Props {
    user?: CorpUser | CorpGroup;
    size?: AvatarSizeOptions;
    entityRegistry: EntityRegistry;
}

export default function AvatarPillWithLinkAndHover({ user, size, entityRegistry }: Props) {
    if (!user) return null;

    const avatarUrl = user.editableProperties?.pictureLink;

    return (
        <HoverEntityTooltip entity={user} showArrow={false}>
            <Link
                to={`${entityRegistry.getEntityUrl(user.type, user.urn)}`}
                onClick={(e) => {
                    e.stopPropagation();
                }}
            >
                <Avatar
                    name={entityRegistry.getDisplayName(user.type, user)}
                    imageUrl={avatarUrl}
                    size={size}
                    type={user.type === EntityType.CorpUser ? AvatarType.user : AvatarType.group}
                    showInPill
                />
            </Link>
        </HoverEntityTooltip>
    );
}
