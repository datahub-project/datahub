import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { DisplayCount, GroupSectionHeader, GroupSectionTitle } from '@app/entity/shared/SidebarStyledComponents';
import { ExpandedOwner } from '@app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { EditOwnersModal } from '@app/entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';

import { EntityType, Ownership } from '@types';

const TITLE = 'Owners';

const SectionWrapper = styled.div``;

const OwnersWrapper = styled.div`
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
    margin-bottom: 8px;
`;

const AddOwnerButton = styled(Button)``;

type Props = {
    ownership: Ownership;
    refetch: () => Promise<any>;
    urn: string;
};

export default function GroupOwnerSideBarSection({ urn, ownership, refetch }: Props) {
    const [showAddModal, setShowAddModal] = useState(false);
    const ownersEmpty = !ownership?.owners?.length;

    return (
        <>
            <GroupSectionHeader>
                <GroupSectionTitle>{TITLE}</GroupSectionTitle>
                <DisplayCount>{ownership?.owners?.length || ''}</DisplayCount>
            </GroupSectionHeader>
            <SectionWrapper>
                <OwnersWrapper>
                    {ownership &&
                        ownership?.owners?.map((owner) => (
                            <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} />
                        ))}
                </OwnersWrapper>
                {ownersEmpty && (
                    <Typography.Paragraph type="secondary">No group owners added yet.</Typography.Paragraph>
                )}
                {ownersEmpty && (
                    <AddOwnerButton onClick={() => setShowAddModal(true)}>
                        <PlusOutlined />
                        Add Owners
                    </AddOwnerButton>
                )}
                {!ownersEmpty && (
                    <AddOwnerButton type="text" style={{ padding: 0 }} onClick={() => setShowAddModal(true)}>
                        <PlusOutlined />
                        Add Owners
                    </AddOwnerButton>
                )}
            </SectionWrapper>
            {showAddModal && (
                <EditOwnersModal
                    urns={[urn]}
                    hideOwnerType
                    entityType={EntityType.CorpGroup}
                    refetch={refetch}
                    onCloseModal={() => {
                        setShowAddModal(false);
                    }}
                />
            )}
        </>
    );
}
