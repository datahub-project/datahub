import { Avatar } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType, OwnerType } from '@types';

const StyledTitle = styled(HeaderTitle)`
    margin-bottom: 12px;
    font-size: 14px;
    color: ${(props) => props.theme.colors.text};
    font-weight: 700;
`;

const Details = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
    font-weight: 500;
`;

const SectionContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    owner: OwnerType;
}

export default function SummaryCreatedBySection({ owner }: Props) {
    // This section shows ownership.owners[0], which is not the entity's creator — DataHub records no
    // human creator for ingested assets, and owners[0] is arbitrary (insertion order). Label it "Owner"
    // rather than the misleading "Created By".
    const { t } = useTranslation('common.labels');
    const entityRegistry = useEntityRegistryV2();

    const ownerName = owner && entityRegistry.getDisplayName(owner.type, owner);
    const ownerPictureLink =
        (owner && 'editableProperties' in owner && owner.editableProperties?.pictureLink) || undefined;
    const avatarType = owner?.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;

    return (
        <>
            {!!ownerName && (
                <SectionContainer>
                    <StyledTitle>{t('owner')}</StyledTitle>
                    <Details>
                        <Avatar name={ownerName} imageUrl={ownerPictureLink} type={avatarType} />
                        {ownerName}
                    </Details>
                </SectionContainer>
            )}
        </>
    );
}
