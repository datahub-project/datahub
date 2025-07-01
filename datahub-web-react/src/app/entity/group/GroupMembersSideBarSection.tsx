import { Tag, Tooltip } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import {
    DisplayCount,
    EmptyValue,
    GroupSectionHeader,
    GroupSectionTitle,
    GroupsSeeMoreText,
    TagsSection,
} from '@app/entity/shared/SidebarStyledComponents';
import { CustomAvatar } from '@app/shared/avatar';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, EntityRelationship, EntityType } from '@types';

const TITLE = 'Members';

const MemberTag = styled(Tag)`
    padding: 2px;
    padding-right: 6px;
    margin-bottom: 8px;
    display: inline-flex;
    align-items: center;
`;

type Props = {
    total: number;
    relationships: Array<EntityRelationship>;
    onSeeMore: () => void;
};

export default function GroupMembersSideBarSection({ total, relationships, onSeeMore }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <GroupSectionHeader>
                <GroupSectionTitle>{TITLE}</GroupSectionTitle>
                <DisplayCount>{total}</DisplayCount>
            </GroupSectionHeader>
            <TagsSection>
                {relationships.length === 0 && <EmptyValue />}
                {relationships.length > 0 &&
                    relationships.map((item) => {
                        const user = item.entity as CorpUser;
                        const name = entityRegistry.getDisplayName(EntityType.CorpUser, user);
                        return (
                            <MemberTag key={user.urn}>
                                <Link to={`${entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}`}>
                                    <CustomAvatar
                                        name={name}
                                        photoUrl={user.editableProperties?.pictureLink || undefined}
                                        useDefaultAvatar={false}
                                    />
                                    {name.length > 15 ? (
                                        <Tooltip title={name}>{`${name.substring(0, 15)}..`}</Tooltip>
                                    ) : (
                                        <span>{name}</span>
                                    )}
                                </Link>
                            </MemberTag>
                        );
                    })}
                {relationships.length > 15 && (
                    <div>
                        <GroupsSeeMoreText onClick={onSeeMore}>{`+${
                            relationships.length - 15
                        } more`}</GroupsSeeMoreText>
                    </div>
                )}
            </TagsSection>
        </>
    );
}
