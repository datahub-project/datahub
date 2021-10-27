import React from 'react';
import { EntityType, Owner } from '../../../types.generated';
import CustomAvatar from './CustomAvatar';
import EntityRegistry from '../../entity/EntityRegistry';
import { SpacedAvatarGroup } from './SpaceAvatarGroup';

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
                            name={
                                owner.owner.info?.fullName ||
                                owner.owner.info?.firstName ||
                                owner.owner.info?.email ||
                                owner.owner.username
                            }
                            url={`/${entityRegistry.getPathName(owner.owner.type)}/${owner.owner.urn}`}
                            photoUrl={owner.owner?.editableInfo?.pictureLink || undefined}
                        />
                    ) : (
                        owner.owner.__typename === 'CorpGroup' && (
                            <CustomAvatar
                                size={size || 28}
                                name={owner.owner.name}
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
