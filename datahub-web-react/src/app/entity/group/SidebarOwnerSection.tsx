import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Ownership } from '../../../types.generated';
import { ExpandedOwner } from '../shared/components/styled/ExpandedOwner';
import { AddOwnerModal } from '../shared/containers/profile/sidebar/Ownership/AddOwnerModal';

type Props = {
    OwnerData: Ownership;
    refetch: () => Promise<any>;
    urn: string;
};

// TODO: get below variables from separate file
const TITLE = 'Owners';
const EMPTY_MESSAGES = {
    owners: {
        title: 'No owners added yet',
        description: 'Adding owners helps you keep track of who is responsible for this data.',
    },
};

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
export default function SidebarOwnerSection({ OwnerData, refetch, urn }: Props) {
    const [showAddModal, setShowAddModal] = useState(false);
    const ownersEmpty = !OwnerData?.owners?.length;

    return (
        <>
            <SectionTitle>
                <Typography.Title level={5}>{TITLE}</Typography.Title>
            </SectionTitle>
            <SectionWrapper>
                {OwnerData &&
                    OwnerData?.owners?.map((owner) => (
                        <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} />
                    ))}
                {ownersEmpty && (
                    <Typography.Paragraph type="secondary">
                        {EMPTY_MESSAGES.owners.title}. {EMPTY_MESSAGES.owners.description}
                    </Typography.Paragraph>
                )}

                <Button type={ownersEmpty ? 'default' : 'text'} onClick={() => setShowAddModal(true)}>
                    <PlusOutlined /> Add Owner
                </Button>
            </SectionWrapper>
            <AddOwnerModal
                visible={showAddModal}
                refetch={refetch}
                onClose={() => {
                    setShowAddModal(false);
                }}
            />
        </>
    );
}
