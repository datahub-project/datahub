import { Avatar, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpGroup, CorpUser, EntityType } from '@types';

const StyledTitle = styled(HeaderTitle)`
    margin-bottom: 12px;
    font-size: 14px;
    color: ${colors.gray[1700]};
    font-weight: 700;
`;

const Details = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 500;
`;

const SectionContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    owner: CorpUser | CorpGroup;
}

export default function SummaryCreatedBySection({ owner }: Props) {
    const entityRegistry = useEntityRegistryV2();

    let ownerName;
    if (owner?.__typename === 'CorpGroup') {
        ownerName = entityRegistry.getDisplayName(EntityType.CorpGroup, owner);
    }
    if (owner?.__typename === 'CorpUser') {
        ownerName = entityRegistry.getDisplayName(EntityType.CorpUser, owner);
    }
    const ownerPictureLink =
        (owner && owner.__typename === 'CorpUser' && owner.editableProperties?.pictureLink) || undefined;
    const avatarType = owner?.__typename === 'CorpGroup' ? AvatarType.group : AvatarType.user;

    return (
        <>
            {!!ownerName && (
                <SectionContainer>
                    <StyledTitle>Created By</StyledTitle>
                    <Details>
                        <Avatar name={ownerName} imageUrl={ownerPictureLink} type={avatarType} />
                        {ownerName}
                    </Details>
                </SectionContainer>
            )}
        </>
    );
}
