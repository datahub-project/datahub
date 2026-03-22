import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';
import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';

import EntityRegistry from '@app/entityV2/EntityRegistry';

import { EntityType } from '@types';

type OwnerLike = {
    owner: {
        type: EntityType;
        urn: string;
        editableProperties?: { pictureLink?: string | null } | null;
        [key: string]: any;
    };
};

type Props = {
    owners: OwnerLike[];
    entityRegistry: EntityRegistry;
};

export const OwnerAvatarGroup = ({ owners, entityRegistry }: Props) => {
    if (owners.length === 0) return null;

    const singleOwner = owners.length === 1 ? owners[0].owner : undefined;
    const ownerAvatars = owners.map((o) => ({
        name: entityRegistry.getDisplayName(o.owner.type, o.owner),
        imageUrl: (o.owner as any).editableProperties?.pictureLink,
        type: mapEntityTypeToAvatarType(o.owner.type),
        urn: o.owner.urn,
    }));

    return (
        <>
            {singleOwner && (
                <Link
                    to={entityRegistry.getEntityUrl(singleOwner.type, singleOwner.urn)}
                    onClick={(e) => e.stopPropagation()}
                >
                    <Avatar
                        name={entityRegistry.getDisplayName(singleOwner.type, singleOwner)}
                        imageUrl={(singleOwner as any).editableProperties?.pictureLink}
                        showInPill
                        type={mapEntityTypeToAvatarType(singleOwner.type)}
                    />
                </Link>
            )}
            {owners.length > 1 && (
                <AvatarStackWithHover avatars={ownerAvatars} showRemainingNumber entityRegistry={entityRegistry} />
            )}
        </>
    );
};
