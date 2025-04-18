import React from 'react';
import { Tag } from 'antd';
import { Tooltip } from '@components';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { CustomAvatar } from '../../shared/avatar';
import { CorpUser, EntityType } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';

const MemberTag = styled(Tag)`
    padding: 2px;
    padding-right: 6px;
    margin-bottom: 8px;
    display: inline-flex;
    width: auto;
`;

type Props = {
    user: CorpUser;
    entityRegistry: EntityRegistry;
};

export const GroupMemberLink = ({ user, entityRegistry }: Props) => {
    const name = entityRegistry.getDisplayName(EntityType.CorpUser, user);
    return (
        <MemberTag key={user.urn}>
            <Link to={`${entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}`}>
                <CustomAvatar
                    name={name}
                    photoUrl={user.editableProperties?.pictureLink || undefined}
                    useDefaultAvatar={false}
                />
                {name.length > 15 ? <Tooltip title={name}>{`${name.substring(0, 15)}..`}</Tooltip> : name}
            </Link>
        </MemberTag>
    );
};
