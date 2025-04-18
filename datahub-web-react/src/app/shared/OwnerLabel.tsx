import React from 'react';
import styled from 'styled-components';
import { EntityType } from '../../types.generated';
import { CustomAvatar } from './avatar';

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

    return (
        <OwnerContainerWrapper>
            <OwnerContentWrapper>
                <CustomAvatar size={24} name={name} photoUrl={avatarUrl} isGroup={type === EntityType.CorpGroup} />
                <div>
                    <div>{name}</div>
                    {subHeader && <div style={{ color: 'gray' }}>{subHeader}</div>}
                </div>
            </OwnerContentWrapper>
        </OwnerContainerWrapper>
    );
};
