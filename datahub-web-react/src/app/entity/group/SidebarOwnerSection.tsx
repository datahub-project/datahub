import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { EntityType, Ownership } from '../../../types.generated';
import { ExpandedOwner } from '../shared/components/styled/ExpandedOwner';
import { AddOwnerModal } from '../shared/containers/profile/sidebar/Ownership/AddOwnerModal';

// TODO: get below variables from separate file
const TITLE = 'Owners';

/**
 * Styled Components
 */
const SectionTitle = styled.div`
    min-height: 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
    > .ant-typography {
        margin-bottom: 0;
    }
`;

const SectionWrapper = styled.div`
    height: calc(75vh - 464px);
`;

type Props = {
    ownership: Ownership;
    refetch: () => Promise<any>;
    urn: string;
};

export default function SidebarOwnerSection({ urn, ownership, refetch }: Props) {
    const [showAddModal, setShowAddModal] = useState(false);
    const ownersEmpty = !ownership?.owners?.length;

    return (
        <>
            <SectionTitle>
                <Typography.Title level={5}>{TITLE}</Typography.Title>
            </SectionTitle>
            <SectionWrapper>
                {ownership &&
                    ownership?.owners?.map((owner) => (
                        <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} />
                    ))}
                {ownersEmpty && (
                    <Typography.Paragraph type="secondary">No group owners added yet.</Typography.Paragraph>
                )}

                <Button type={ownersEmpty ? 'default' : 'text'} onClick={() => setShowAddModal(true)}>
                    <PlusOutlined /> Add Owner
                </Button>
            </SectionWrapper>
            <AddOwnerModal
                urn={urn}
                type={EntityType.CorpGroup}
                visible={showAddModal}
                refetch={refetch}
                onClose={() => {
                    setShowAddModal(false);
                }}
            />
        </>
    );
}
