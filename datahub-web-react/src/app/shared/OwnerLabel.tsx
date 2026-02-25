import React from 'react';
import styled, { useTheme } from 'styled-components';

import { CustomAvatar } from '@app/shared/avatar';

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
    const theme = useTheme();
    const subHeader = [corpUserId, corpUserTitle, corpUserDepartmentName].filter(Boolean).join(' - ');

    return (
        <OwnerContainerWrapper>
            <OwnerContentWrapper>
                <CustomAvatar size={24} name={name} photoUrl={avatarUrl} isGroup={type === EntityType.CorpGroup} />
                <div>
                    <div>{name}</div>
                    {subHeader && <div style={{ color: theme.colors.textTertiary }}>{subHeader}</div>}
                </div>
            </OwnerContentWrapper>
        </OwnerContainerWrapper>
    );
};
