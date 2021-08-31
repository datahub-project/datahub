import { Avatar } from 'antd';
import { AvatarSize } from 'antd/lib/avatar/SizeContext';
import React from 'react';
import { EntityType, Owner } from '../../../types.generated';
import CustomAvatar from './CustomAvatar';
import EntityRegistry from '../../entity/EntityRegistry';

type Props = {
    owners?: Array<Owner> | null;
    entityRegistry: EntityRegistry;
    maxCount?: number;
    size?: AvatarSize;
};

export default function AvatarsGroup({ owners, entityRegistry, maxCount = 6, size = 'default' }: Props) {
    if (!owners || owners.length === 0) {
        return null;
    }
    return (
        <Avatar.Group maxCount={maxCount} size={size}>
            {(owners || [])?.map((owner, key) => (
                // eslint-disable-next-line react/no-array-index-key
                <div data-testid={`avatar-tag-${owner.owner.urn}`} key={`${owner.owner.urn}-${key}`}>
                    {owner.owner.__typename === 'CorpUser' ? (
                        <CustomAvatar
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
        </Avatar.Group>
    );
}
