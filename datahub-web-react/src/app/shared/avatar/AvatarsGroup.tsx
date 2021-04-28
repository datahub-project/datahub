import { Avatar } from 'antd';
import { AvatarSize } from 'antd/lib/avatar/SizeContext';
import React from 'react';
import { CorpUser, Owner } from '../../../types.generated';
import CustomAvatar from './CustomAvatar';
import EntityRegistry from '../../entity/EntityRegistry';

type Props = {
    owners?: Array<Owner> | null;
    entityRegistry: EntityRegistry;
    maxCount?: number;
    size?: AvatarSize;
};

export default function AvatarsGroup({ owners, entityRegistry, maxCount = 6, size = 'default' }: Props) {
    // TODO: update owner to support corpuser and corpgroup
    return (
        <Avatar.Group maxCount={maxCount} size={size}>
            {(owners || [])?.map((owner) => (
                <div data-testid={`avatar-tag-${owner.owner.urn}`} key={owner.owner.urn}>
                    <CustomAvatar
                        name={(owner.owner as CorpUser).info?.fullName || undefined}
                        url={`/${entityRegistry.getPathName(owner.owner.type)}/${(owner.owner as CorpUser).urn}`}
                        photoUrl={(owner.owner as CorpUser)?.editableInfo?.pictureLink || undefined}
                    />
                </div>
            ))}
        </Avatar.Group>
    );
}
