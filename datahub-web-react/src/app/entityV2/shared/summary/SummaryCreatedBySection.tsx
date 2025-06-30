import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpGroup, CorpUser, EntityType } from '@types';

const StyledTitle = styled(HeaderTitle)`
    margin-bottom: 12px;
    font-size: 14px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-weight: 700;
`;

const Details = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
    color: ${REDESIGN_COLORS.SUBTITLE};
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

    return (
        <>
            {!!ownerName && (
                <SectionContainer>
                    <StyledTitle>Created By</StyledTitle>
                    <Details>
                        {!!ownerPictureLink && <CustomAvatar photoUrl={ownerPictureLink} size={28} useDefaultAvatar />}
                        {ownerName}
                    </Details>
                </SectionContainer>
            )}
        </>
    );
}
