import React from 'react';
import GroupMembersSideBarSectionContent from './GroupMembersSidebarSectionContent';
import { SidebarSection } from '../shared/containers/profile/sidebar/SidebarSection';
import { EntityRelationshipsResult } from '../../../types.generated';

type Props = {
    groupMemberRelationships: EntityRelationshipsResult;
};

export const GroupSidebarMembersSection = ({ groupMemberRelationships }: Props) => {
    return (
        <SidebarSection
            title="Members"
            count={groupMemberRelationships?.total || undefined}
            content={
                <GroupMembersSideBarSectionContent relationships={groupMemberRelationships?.relationships || []} />
            }
        />
    );
};
