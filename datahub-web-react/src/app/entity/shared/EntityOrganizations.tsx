import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';

import { EmptyValue, GroupsSeeMoreText, Tags, TagsSection } from '@app/entity/shared/SidebarStyledComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityRelationship, EntityType } from '@types';

type Props = {
    readMore: boolean;
    setReadMore: (readMore: boolean) => void;
    organizationRelationships: Array<EntityRelationship>;
};

/**
 * EntityOrganizations - to display the organizations category in sidebar section
 */
export function EntityOrganizations({ readMore, setReadMore, organizationRelationships }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <TagsSection>
            {organizationRelationships?.length === 0 && <EmptyValue />}
            {!readMore &&
                organizationRelationships?.slice(0, 2).map((item) => {
                    if (!item?.entity?.urn) return null;
                    const entityUrn = entityRegistry.getEntityUrl(EntityType.Organization, item?.entity?.urn);
                    return (
                        <Link to={entityUrn} key={entityUrn}>
                            <Tags>
                                <Tag>{entityRegistry.getDisplayName(EntityType.Organization, item.entity)}</Tag>
                            </Tags>
                        </Link>
                    );
                })}
            {readMore &&
                organizationRelationships?.length > 2 &&
                organizationRelationships?.map((item) => {
                    if (!item?.entity?.urn) return null;
                    const entityUrn = entityRegistry.getEntityUrl(EntityType.Organization, item.entity.urn);
                    return (
                        <Link to={entityUrn} key={entityUrn}>
                            <Tags>
                                <Tag>{entityRegistry.getDisplayName(EntityType.Organization, item.entity)}</Tag>
                            </Tags>
                        </Link>
                    );
                })}
            {!readMore && organizationRelationships?.length > 2 && (
                <GroupsSeeMoreText onClick={() => setReadMore(!readMore)}>
                    {`+${organizationRelationships?.length - 2} more`}
                </GroupsSeeMoreText>
            )}
        </TagsSection>
    );
}
