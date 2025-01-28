import React, { useState } from 'react';
import { Typography } from 'antd';
import { CorpUser, EntityRelationship } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { TagsSection } from '../shared/SidebarStyledComponents';
import { ShowMoreSection } from '../shared/sidebarSection/ShowMoreSection';
import { GroupMemberLink } from './GroupMemberLink';

type Props = {
    relationships: Array<EntityRelationship>;
};
const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

export default function GroupMembersSidebarSectionContent({ relationships }: Props) {
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);

    const entityRegistry = useEntityRegistry();
    const relationshipsCount = relationships?.length || 0;
    return (
        <>
            <TagsSection>
                {relationships.length === 0 && (
                    <Typography.Paragraph type="secondary">No members yet.</Typography.Paragraph>
                )}
                {relationships.length > 0 &&
                    relationships.map((item, index) => {
                        const user = item.entity as CorpUser;
                        return index < entityCount && <GroupMemberLink user={user} entityRegistry={entityRegistry} />;
                    })}
            </TagsSection>
            {relationshipsCount > entityCount && (
                <ShowMoreSection
                    totalCount={relationshipsCount}
                    entityCount={entityCount}
                    setEntityCount={setEntityCount}
                    showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                />
            )}
        </>
    );
}
