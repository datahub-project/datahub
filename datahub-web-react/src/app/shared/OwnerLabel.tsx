import { Avatar, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { EntityType } from '@types';

const OwnerContainerWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const OwnerContentWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 4px;
`;

const SubHeader = styled.div`
    color: ${colors.gray[1700]};
    font-size: 12px;
`;

type Props = {
    name: string;
    avatarUrl: string | undefined;
    type: EntityType;
    corpUserId?: string;
    corpUserTitle?: string;
    corpUserDepartmentName?: string;
};

export const OwnerLabel = ({ name, avatarUrl, type, corpUserId, corpUserTitle, corpUserDepartmentName }: Props) => {
    const subHeader = [corpUserId, corpUserTitle, corpUserDepartmentName].filter(Boolean).join(' - ');
    const avatarType = type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;

    return (
        <OwnerContainerWrapper>
            <OwnerContentWrapper>
                <Avatar name={name} imageUrl={avatarUrl} type={avatarType} />
                <div>
                    <div>{name}</div>
                    {subHeader && <SubHeader>{subHeader}</SubHeader>}
                </div>
            </OwnerContentWrapper>
        </OwnerContainerWrapper>
    );
};
