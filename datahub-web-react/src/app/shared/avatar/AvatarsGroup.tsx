import React from 'react';

import EntityRegistry from '@app/entity/EntityRegistry';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';
import { SpacedAvatarGroup } from '@app/shared/avatar/SpaceAvatarGroup';

import { EntityType, Owner } from '@types';

type Props = {
    owners?: Array<Owner> | null;
    entityRegistry: EntityRegistry;
    maxCount?: number;
    size?: number;
};

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export default function AvatarsGroup({ owners, entityRegistry, maxCount = 6, size }: Props) {
    if (!owners || owners.length === 0) {
        return null;
    }
    return (
        <SpacedAvatarGroup maxCount={maxCount}>
            {(owners || [])?.map((owner, key) => (
                // eslint-disable-next-line react/no-array-index-key
                <div data-testid={`avatar-tag-${owner.owner.urn}`} key={`${owner.owner.urn}-${key}`}>
                    {owner.owner.__typename === 'CorpUser' ? (
                        <CustomAvatar
                            size={size}
                            name={entityRegistry.getDisplayName(EntityType.CorpUser, owner.owner)}
                            url={`/${entityRegistry.getPathName(owner.owner.type)}/${owner.owner.urn}`}
                            photoUrl={
                                owner.owner?.editableProperties?.pictureLink ||
                                owner.owner?.editableInfo?.pictureLink ||
                                undefined
                            }
                        />
                    ) : (
                        owner.owner.__typename === 'CorpGroup' && (
                            <CustomAvatar
                                size={size || 28}
                                name={entityRegistry.getDisplayName(EntityType.CorpGroup, owner.owner)}
                                url={`/${entityRegistry.getPathName(owner.owner.type || EntityType.CorpGroup)}/${
                                    owner.owner.urn
                                }`}
                                isGroup
                            />
                        )
                    )}
                </div>
            ))}
        </SpacedAvatarGroup>
    );
}
