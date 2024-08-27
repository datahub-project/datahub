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
};

export const OwnerLabel = ({ name, avatarUrl, type }: Props) => {
    return (
        <OwnerContainerWrapper>
            <OwnerContentWrapper>
                <CustomAvatar size={24} name={name} photoUrl={avatarUrl} isGroup={type === EntityType.CorpGroup} />
                <div>{name}</div>
            </OwnerContentWrapper>
        </OwnerContainerWrapper>
    );
};
