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

import GroupOwnerSidebarSectionContent from '@app/entityV2/group/GroupOwnerSidebarSectionContent';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

import { Ownership } from '@types';

type Props = {
    ownership: Ownership;
    refetch: () => Promise<any>;
    urn: string;
};

export const GroupSidebarOwnersSection = ({ ownership, refetch, urn }: Props) => {
    const [showAddOwnerModal, setShowAddOwnerModal] = useState(false);

    return (
        <SidebarSection
            title="Owners"
            count={ownership?.owners?.length}
            content={
                <GroupOwnerSidebarSectionContent
                    ownership={ownership}
                    urn={urn || ''}
                    refetch={refetch}
                    showAddOwnerModal={showAddOwnerModal}
                    setShowAddOwnerModal={setShowAddOwnerModal}
                />
            }
            extra={
                <SectionActionButton
                    button={<PlusOutlined data-testid="add-owners-sidebar-button" />}
                    onClick={(event) => {
                        setShowAddOwnerModal(true);
                        event.stopPropagation();
                    }}
                />
            }
        />
    );
};
