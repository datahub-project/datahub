import React, { useMemo } from 'react';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import EntityRegistry from '@app/entity/EntityRegistry';

import { CorpGroup, CorpUser, DataHubPolicy, DataHubRole, EntityType, Maybe } from '@types';

type Props = {
    users?: Maybe<Array<CorpUser>>;
    groups?: Maybe<Array<CorpGroup>>;
    policies?: Maybe<Array<DataHubPolicy>>;
    roles?: Maybe<Array<DataHubRole>>;
    entityRegistry: EntityRegistry;
    maxCount?: number;
};

export default function AvatarsGroup({ users, groups, policies, roles, entityRegistry, maxCount = 6 }: Props) {
    const avatars: AvatarItemProps[] = useMemo(() => {
        const result: AvatarItemProps[] = [];

        users?.forEach((user) => {
            result.push({
                name: entityRegistry.getDisplayName(EntityType.CorpUser, user),
                imageUrl: user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined,
                urn: user?.urn,
                type: AvatarType.user,
            });
        });

        groups?.forEach((group) => {
            result.push({
                name: entityRegistry.getDisplayName(EntityType.CorpGroup, group),
                urn: group.urn,
                type: AvatarType.group,
            });
        });

        roles?.forEach((role) => {
            result.push({
                name: role.name,
                urn: role.urn,
                type: AvatarType.role,
            });
        });

        policies?.forEach((policy) => {
            result.push({
                name: policy.name,
                urn: policy.urn,
                type: AvatarType.user,
            });
        });

        return result;
    }, [users, groups, roles, policies, entityRegistry]);

    return <AvatarStack avatars={avatars} maxToShow={maxCount} />;
}
