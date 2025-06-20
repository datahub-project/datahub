import { Typography } from 'antd';
import React, { useState } from 'react';
import { useHistory } from 'react-router';
import { useRouteMatch } from 'react-router-dom';

import { GroupMemberLink } from '@app/entityV2/group/GroupMemberLink';
import { TabType } from '@app/entityV2/group/types';
import { ShowMoreButton, TagsSection } from '@app/entityV2/shared/SidebarStyledComponents';
import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, EntityRelationshipsResult } from '@types';

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
    const relationshipsAvailableCount = groupMemberRelationships?.relationships?.length || 0;

    const hasHiddenEntities = relationshipsTotal > relationshipsAvailableCount;
    const isShowingMaxEntities = entityCount >= relationshipsAvailableCount;
    const showAndMoreText = hasHiddenEntities && isShowingMaxEntities;

    return (
        <>
            <TagsSection>
                {relationshipsTotal === 0 && (
                    <Typography.Paragraph type="secondary">No members yet.</Typography.Paragraph>
                )}
                {relationshipsTotal > 0 &&
                    groupMemberRelationships?.relationships?.map((item, index) => {
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
            {showAndMoreText && (
                <ShowMoreButton onClick={() => history.replace(`${url}/${TabType.Members.toLocaleLowerCase()}`)}>
                    View all members
                </ShowMoreButton>
            )}
        </>
    );
}
