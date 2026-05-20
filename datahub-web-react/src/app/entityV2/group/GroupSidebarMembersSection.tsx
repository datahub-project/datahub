import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';

import GroupMembersSideBarSectionContent from '@app/entityV2/group/GroupMembersSidebarSectionContent';
import { getExternalGroupMembershipTooltip } from '@app/entityV2/group/utils';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

import { EntityRelationshipsResult } from '@types';

type Props = {
    groupMemberRelationships?: EntityRelationshipsResult;
    urn: string;
    refetch: () => void;
    isExternalGroup?: boolean;
    externalGroupType?: string;
};

export const GroupSidebarMembersSection = ({
    groupMemberRelationships,
    urn,
    refetch,
    isExternalGroup = false,
    externalGroupType,
}: Props) => {
    const [showAddMemberModal, setShowAddMemberModal] = useState(false);
    const externalGroupTip = isExternalGroup ? getExternalGroupMembershipTooltip(externalGroupType) : undefined;
    return (
        <SidebarSection
            title="Members"
            count={groupMemberRelationships?.total || undefined}
            showFullCount
            content={
                <GroupMembersSideBarSectionContent
                    groupMemberRelationships={groupMemberRelationships}
                    showAddMemberModal={showAddMemberModal}
                    setShowAddMemberModal={setShowAddMemberModal}
                    urn={urn}
                    refetch={refetch}
                />
            }
            extra={
                <SectionActionButton
                    icon={Plus}
                    onClick={(event) => {
                        event.stopPropagation();
                        if (isExternalGroup) return;
                        setShowAddMemberModal(true);
                    }}
                    actionPrivilege={!isExternalGroup}
                    tip={externalGroupTip}
                    dataTestId="add-group-members-sidebar-button"
                />
            }
        />
    );
};
