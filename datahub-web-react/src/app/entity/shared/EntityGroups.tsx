import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { EntityType, EntityRelationship } from '../../../types.generated';
import { EmptyValue, TagsSection, Tags, GroupsSeeMoreText } from './SidebarStyledComponents';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    readMore: boolean;
    setReadMore: (readMore: boolean) => void;
    groupMemberRelationships: Array<EntityRelationship>;
};

/**
 * EntityGroups- to display the groups category in sidebar section
 */
export default function EntityGroups({ readMore, setReadMore, groupMemberRelationships }: Props) {
    const entityRegistry = useEntityRegistry();
    const sortedGroupMemberRelationships = groupMemberRelationships?.sort((group1, group2) => {
        const groupName1 = entityRegistry.getDisplayName(EntityType.CorpGroup, group1?.entity) ?? '';
        const groupName2 = entityRegistry.getDisplayName(EntityType.CorpGroup, group2?.entity) ?? '';
        if (groupName1 < groupName2) {
            return -1;
        }
        return 1;
    });

    return (
        <TagsSection>
            {groupMemberRelationships?.length === 0 && <EmptyValue />}
            {!readMore &&
                sortedGroupMemberRelationships?.slice(0, 2).map((item) => {
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
                groupMemberRelationships?.length > 2 &&
                sortedGroupMemberRelationships?.map((item) => {
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
            {!readMore && groupMemberRelationships?.length > 2 && (
                <GroupsSeeMoreText onClick={() => setReadMore(!readMore)}>
                    {`+${groupMemberRelationships?.length - 2} more`}
                </GroupsSeeMoreText>
            )}
        </TagsSection>
    );
}
