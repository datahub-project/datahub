import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { EntityType, Ownership } from '../../../types.generated';
import { ExpandedOwner } from '../shared/components/styled/ExpandedOwner';
import { AddOwnersModal } from '../shared/containers/profile/sidebar/Ownership/AddOwnersModal';
import { DisplayCount, GroupSectionTitle, GroupSectionHeader } from '../shared/SidebarStyledComponents';

const TITLE = 'Owners';

const SectionWrapper = styled.div``;

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
                {ownership &&
                    ownership?.owners?.map((owner) => (
                        <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} />
                    ))}
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
                <AddOwnersModal
                    urn={urn}
                    hideOwnerType
                    type={EntityType.CorpGroup}
                    refetch={refetch}
                    onCloseModal={() => {
                        setShowAddModal(false);
                    }}
                />
            )}
        </>
    );
}
