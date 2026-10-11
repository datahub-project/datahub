import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import EntityRegistry from '@app/entity/EntityRegistry';

import { CorpUser, EntityType } from '@types';

const MemberPill = styled.div`
    display: inline-flex;
    margin-bottom: 8px;
`;

type Props = {
    user: CorpUser;
    entityRegistry: EntityRegistry;
};

export const GroupMemberLink = ({ user, entityRegistry }: Props) => {
    const name = entityRegistry.getDisplayName(EntityType.CorpUser, user);
    return (
        <MemberPill key={user.urn}>
            <Link to={`${entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}`}>
                <Avatar
                    name={name}
                    imageUrl={user.editableProperties?.pictureLink || undefined}
                    type={AvatarType.user}
                    showInPill
                />
            </Link>
        </MemberPill>
    );
};
