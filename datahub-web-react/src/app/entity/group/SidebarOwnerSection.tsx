import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { AddOwnerModal } from '../shared/containers/profile/sidebar/Ownership/AddOwnerModal';

type Props = {
    data: string;
    refetch: () => Promise<any>;
};

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
    color: red;
`;
export default function SidebarOwnerSection({ data, refetch }: Props) {
    const title = 'Owners';
    const ownersEmpty = true;
    const EMPTY_MESSAGES = {
        owners: {
            title: 'No owners added yet',
            description: 'Adding owners helps you keep track of who is responsible for this data.',
        },
    };
    const [showAddModal, setShowAddModal] = useState(false);
    console.log('data', data);
    return (
        <>
            <SectionTitle>
                <Typography.Title level={5}>{title}</Typography.Title>
            </SectionTitle>
            <SectionWrapper>
                {/* {entityData?.ownership?.owners?.map((owner) => (
                    <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} />
                ))} */}
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
