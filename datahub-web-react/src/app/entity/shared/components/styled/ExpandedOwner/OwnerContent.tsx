import { Avatar } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { AvatarType } from '@components/components/AvatarStack/types';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';

import { EntityType, Owner } from '@types';

const TextWrapper = styled.span<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
`;

const ContentWrapper = styled.span`
    display: flex;
    align-items: center;
    gap: 4px;
`;

interface Props {
    name: string;
    owner: Owner;
    hidePopOver?: boolean;
    pictureLink?: string;
    fontSize?: number;
}

export default function OwnerContent({ name, owner, hidePopOver, pictureLink, fontSize }: Props) {
    const ownerEntityType = owner.owner.type;
    const avatarType = ownerEntityType === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;

    const label = <TextWrapper fontSize={fontSize}>{name}</TextWrapper>;

    return (
        <ContentWrapper>
            <Avatar name={name} imageUrl={pictureLink} type={avatarType} />
            {hidePopOver ? label : <HoverEntityTooltip entity={owner.owner}>{label}</HoverEntityTooltip>}
        </ContentWrapper>
    );
}
