/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PlusOutlined } from '@ant-design/icons';
import React, { useState } from 'react';

import GroupMembersSideBarSectionContent from '@app/entityV2/group/GroupMembersSidebarSectionContent';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

import { EntityRelationshipsResult } from '@types';

type Props = {
    groupMemberRelationships: EntityRelationshipsResult;
    urn: string;
    refetch: () => void;
};

export const GroupSidebarMembersSection = ({ groupMemberRelationships, urn, refetch }: Props) => {
    const [showAddMemberModal, setShowAddMemberModal] = useState(false);
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
                    button={<PlusOutlined />}
                    onClick={(event) => {
                        setShowAddMemberModal(true);
                        event.stopPropagation();
                    }}
                />
            }
        />
    );
};
