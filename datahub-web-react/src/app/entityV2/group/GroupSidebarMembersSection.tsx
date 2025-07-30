import React from 'react';

import GroupMembersSideBarSectionContent from '@app/entityV2/group/GroupMembersSidebarSectionContent';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

import { EntityRelationshipsResult } from '@types';

type Props = {
    groupMemberRelationships: EntityRelationshipsResult;
};

export const GroupSidebarMembersSection = ({ groupMemberRelationships }: Props) => {
    return (
        <SidebarSection
            title="Members"
            count={groupMemberRelationships?.total || undefined}
            showFullCount
            content={<GroupMembersSideBarSectionContent groupMemberRelationships={groupMemberRelationships} />}
        />
    );
};
