import React, { useState } from 'react';
import { CorpUser, EntityRelationship } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EmptyValue, TagsSection } from '../shared/SidebarStyledComponents';
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
        <TagsSection>
            {relationships.length === 0 && <EmptyValue />}
            {relationships.length > 0 &&
                relationships.map((item, index) => {
                    const user = item.entity as CorpUser;
                    return index < entityCount && <GroupMemberLink user={user} entityRegistry={entityRegistry} />;
                })}
            {relationshipsCount > entityCount && (
                <ShowMoreSection
                    totalCount={relationshipsCount}
                    entityCount={entityCount}
                    setEntityCount={setEntityCount}
                    showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                />
            )}
        </TagsSection>
    );
}
