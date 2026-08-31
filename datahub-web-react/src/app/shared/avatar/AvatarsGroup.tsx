import React, { useMemo } from 'react';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import EntityRegistry from '@app/entity/EntityRegistry';

import { EntityType, Owner } from '@types';

type Props = {
    owners?: Array<Owner> | null;
    entityRegistry: EntityRegistry;
    maxCount?: number;
};

export default function AvatarsGroup({ owners, entityRegistry, maxCount = 6 }: Props) {
    const avatars: AvatarItemProps[] = useMemo(() => {
        if (!owners) return [];
        return owners
            .map((owner) => {
                if (owner.owner.__typename === 'CorpUser') {
                    return {
                        name: entityRegistry.getDisplayName(EntityType.CorpUser, owner.owner),
                        imageUrl:
                            owner.owner?.editableProperties?.pictureLink ||
                            owner.owner?.editableInfo?.pictureLink ||
                            undefined,
                        urn: owner.owner.urn,
                        type: AvatarType.user,
                    };
                }
                if (owner.owner.__typename === 'CorpGroup') {
                    return {
                        name: entityRegistry.getDisplayName(EntityType.CorpGroup, owner.owner),
                        urn: owner.owner.urn,
                        type: AvatarType.group,
                    };
                }
                return null;
            })
            .filter(Boolean) as AvatarItemProps[];
    }, [owners, entityRegistry]);

    if (!owners || owners.length === 0) {
        return null;
    }

    return <AvatarStack avatars={avatars} maxToShow={maxCount} />;
}
