import { Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import { UpdateDatasetMutation } from '../../../../../graphql/dataset.generated';
import UpdateDescriptionModal from '../../../shared/DescriptionModal';
import MarkdownViewer from '../../../shared/MarkdownViewer';

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: row;
`;

const DescriptionText = styled(MarkdownViewer)`
    padding-right: 8px;
`;

const EditedLabel = styled(Typography.Text)`
    position: absolute;
    right: -10px;
    top: -15px;
    color: rgba(150, 150, 150, 0.5);
    font-style: italic;
`;

type Props = {
    description: string;
    updatedDescription?: string | null;
    onUpdate: (
        description: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>> | void>;
};

export default function DescriptionField({ description, updatedDescription, onUpdate }: Props) {
    const [showAddModal, setShowAddModal] = useState(false);

    const onCloseModal = () => setShowAddModal(false);

    const onUpdateModal = async (desc: string | null) => {
        message.loading({ content: 'Updating...' });
        await onUpdate(desc || '');
        message.success({ content: 'Updated!', duration: 2 });
        onCloseModal();
    };

    return (
        <DescriptionContainer
            onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
            }}
        >
            <DescriptionText
                source={updatedDescription || description}
                editable
                onEditClicked={() => setShowAddModal(true)}
            />
            {updatedDescription && <EditedLabel>(edited)</EditedLabel>}
            {showAddModal && (
                <div>
                    <UpdateDescriptionModal
                        title="Update description"
                        description={updatedDescription || description}
                        original={description}
                        onClose={onCloseModal}
                        onSubmit={onUpdateModal}
                    />
                </div>
            )}
        </DescriptionContainer>
    );
}
