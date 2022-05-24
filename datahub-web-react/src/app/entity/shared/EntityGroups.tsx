import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { EntityType, EntityRelationshipsResult } from '../../../types.generated';
import { EmptyValue, TagsSection, Tags, GroupsSeeMoreText } from './SidebarStyledComponents';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    readMore: boolean;
    setReadMore: (readMore: boolean) => void;
    groupMemberRelationships: EntityRelationshipsResult;
};

/**
 * EntityGroups- to display the groups category in sidebar section
 */
export default function EntityGroups({ readMore, setReadMore, groupMemberRelationships }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <TagsSection>
            {groupMemberRelationships?.relationships.length === 0 && <EmptyValue />}
            {!readMore &&
                groupMemberRelationships?.relationships.slice(0, 2).map((item) => {
                    if (!item?.entity?.urn) return null;
                    const entityUrn = entityRegistry.getEntityUrl(EntityType.CorpGroup, item?.entity?.urn);
                    return (
                        <Link to={entityUrn} key={entityUrn}>
                            <Tags>
                                <Tag>{entityRegistry.getDisplayName(EntityType.CorpGroup, item.entity)}</Tag>
                            </Tags>
                        </Link>
                    );
                })}
            {readMore &&
                groupMemberRelationships?.relationships.length > 2 &&
                groupMemberRelationships?.relationships.map((item) => {
                    if (!item?.entity?.urn) return null;
                    const entityUrn = entityRegistry.getEntityUrl(EntityType.CorpGroup, item.entity.urn);
                    return (
                        <Link to={entityUrn} key={entityUrn}>
                            <Tags>
                                <Tag>{entityRegistry.getDisplayName(EntityType.CorpGroup, item.entity)}</Tag>
                            </Tags>
                        </Link>
                    );
                })}
            {!readMore && groupMemberRelationships?.relationships.length > 2 && (
                <GroupsSeeMoreText onClick={() => setReadMore(!readMore)}>
                    {`+${groupMemberRelationships?.relationships.length - 2} more`}
                </GroupsSeeMoreText>
            )}
        </TagsSection>
    );
}
