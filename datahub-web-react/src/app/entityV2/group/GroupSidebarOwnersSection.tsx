import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import GroupOwnerSidebarSectionContent from './GroupOwnerSidebarSectionContent';
import SectionActionButton from '../shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '../shared/containers/profile/sidebar/SidebarSection';
import { Ownership } from '../../../types.generated';

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
                    button={<PlusOutlined />}
                    onClick={(event) => {
                        setShowAddOwnerModal(true);
                        event.stopPropagation();
                    }}
                />
            }
        />
    );
};
