import React, { useState } from 'react';
import { Typography } from 'antd';
import { useRouteMatch } from 'react-router-dom';
import { useHistory } from 'react-router';
import { CorpUser, EntityRelationshipsResult } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ShowMoreButton, TagsSection } from '../shared/SidebarStyledComponents';
import { ShowMoreSection } from '../shared/sidebarSection/ShowMoreSection';
import { GroupMemberLink } from './GroupMemberLink';
import { TabType } from './types';

type Props = {
    groupMemberRelationships: EntityRelationshipsResult;
};
const DEFAULT_MAX_ENTITIES_TO_SHOW = 5;

export default function GroupMembersSidebarSectionContent({ groupMemberRelationships }: Props) {
    const history = useHistory();
    const { url } = useRouteMatch();
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);

    const entityRegistry = useEntityRegistry();
    const relationshipsTotal = groupMemberRelationships?.total || 0;
    const relationshipsAvailableCount = groupMemberRelationships.relationships?.length || 0;
    return (
        <>
            <TagsSection>
                {relationshipsTotal === 0 && (
                    <Typography.Paragraph type="secondary">No members yet.</Typography.Paragraph>
                )}
                {relationshipsTotal > 0 &&
                    groupMemberRelationships.relationships.map((item, index) => {
                        const user = item.entity as CorpUser;
                        return index < entityCount && <GroupMemberLink user={user} entityRegistry={entityRegistry} />;
                    })}
            </TagsSection>
            {relationshipsAvailableCount > entityCount && (
                <ShowMoreSection
                    totalCount={relationshipsAvailableCount}
                    entityCount={entityCount}
                    setEntityCount={setEntityCount}
                    showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                />
            )}
            {relationshipsTotal > relationshipsAvailableCount && entityCount >= relationshipsAvailableCount && (
                <ShowMoreButton onClick={() => history.replace(`${url}/${TabType.Members.toLocaleLowerCase()}`)}>
                    View all members
                </ShowMoreButton>
            )}
        </>
    );
}
