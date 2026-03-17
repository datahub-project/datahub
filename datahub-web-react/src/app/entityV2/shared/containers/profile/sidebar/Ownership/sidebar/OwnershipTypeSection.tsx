import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import AvatarPillWithLinkAndHover from '@components/components/Avatar/AvatarPillWithLinkAndHover';
import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import { getOwnershipTypeDescription } from '@app/entityV2/shared/components/styled/ExpandedOwner/OwnerUtils';
import { getOwnershipTypeName } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpGroup, CorpUser, EntityType, Owner, OwnershipTypeEntity } from '@types';

const OwnershipTypeContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-right: 12px;
    max-width: inherit;
`;

const OwnershipTypeNameText = styled.span`
    font-family: 'Mulish', sans-serif;
    font-weight: 500;
    font-size: 10px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const OwnersContainer = styled.div`
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    margin-top: 8px;
    max-width: inherit;
    gap: 8px;
`;

interface Props {
    ownershipType: OwnershipTypeEntity;
    owners: Owner[];
}

export const OwnershipTypeSection = ({ ownershipType, owners }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const ownershipTypeName = getOwnershipTypeName(ownershipType);
    const ownershipTypeDescription = getOwnershipTypeDescription(ownershipType);

    const avatars: AvatarItemProps[] = owners.map((owner) => ({
        name: entityRegistry.getDisplayName(
            owner.owner.__typename === 'CorpUser' ? EntityType.CorpUser : EntityType.CorpGroup,
            owner.owner,
        ),
        imageUrl: (owner.owner as CorpUser | CorpGroup).editableProperties?.pictureLink,
        urn: owner.owner.urn,
        type: owner.owner.__typename === 'CorpUser' ? AvatarType.user : AvatarType.group,
    }));

    return (
        <OwnershipTypeContainer>
            <Tooltip title={ownershipTypeDescription}>
                <OwnershipTypeNameText>{ownershipTypeName}</OwnershipTypeNameText>
            </Tooltip>
            <OwnersContainer>
                {owners.length === 1 ? (
                    <AvatarPillWithLinkAndHover
                        user={owners[0].owner as CorpUser | CorpGroup}
                        size="sm"
                        entityRegistry={entityRegistry}
                    />
                ) : (
                    <AvatarStackWithHover
                        avatars={avatars}
                        size="sm"
                        maxToShow={3}
                        entityRegistry={entityRegistry}
                        title={ownershipTypeName}
                    />
                )}
            </OwnersContainer>
        </OwnershipTypeContainer>
    );
};
