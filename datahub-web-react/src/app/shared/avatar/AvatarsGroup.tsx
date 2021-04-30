import { Avatar } from 'antd';
import { AvatarSize } from 'antd/lib/avatar/SizeContext';
import React from 'react';
import { CorpGroup, CorpUser, EntityType, Owner } from '../../../types.generated';
import CustomAvatar from './CustomAvatar';
import EntityRegistry from '../../entity/EntityRegistry';

type Props = {
    owners?: Array<Owner> | null;
    entityRegistry: EntityRegistry;
    maxCount?: number;
    size?: AvatarSize;
};

export default function AvatarsGroup({ owners, entityRegistry, maxCount = 6, size = 'default' }: Props) {
    return (
        <Avatar.Group maxCount={maxCount} size={size}>
            {(owners || [])?.map((owner) => (
                <div data-testid={`avatar-tag-${owner.owner.urn}`} key={owner.owner.urn}>
                    {owner.owner.type === EntityType.CorpUser ? (
                        <CustomAvatar
                            name={
                                (owner.owner as CorpUser).info?.fullName ||
                                (owner.owner as CorpUser).info?.firstName ||
                                (owner.owner as CorpUser).info?.email
                            }
                            url={`/${entityRegistry.getPathName(owner.owner.type)}/${(owner.owner as CorpUser).urn}`}
                            photoUrl={(owner.owner as CorpUser)?.editableInfo?.pictureLink || undefined}
                        />
                    ) : (
                        <CustomAvatar
                            name={(owner.owner as CorpGroup).name || (owner.owner as CorpGroup).info?.email}
                            url={`/${entityRegistry.getPathName(owner.owner.type || EntityType.CorpGroup)}/${
                                (owner.owner as CorpGroup).urn
                            }`}
                            isGroup
                        />
                    )}
                </div>
            ))}
        </Avatar.Group>
    );
}
