import { Typography, message } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';
import MDEditor from '@uiw/react-md-editor';

import { UpdateDatasetMutation } from '../../../../../graphql/dataset.generated';
import UpdateDescriptionModal from '../modal/UpdateDescriptionModal';

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: row;
`;

const DescriptionText = styled(MDEditor.Markdown)`
    padding-right: 8px;
    max-width: 600px;
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
`;

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    margin-top: 4px;
`;

const EditedLabel = styled(Typography.Text)`
    padding-left: 8px;
    color: rgb(150, 150, 150);
    font-style: italic;
`;

const MessageKey = 'UpdateSchemaDescription';

type Props = {
    description: string;
    updatedDescription?: string | null;
    onHover: boolean;
    onUpdate: (
        description: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>> | void>;
};

export default function DescriptionField({ description, updatedDescription, onHover, onUpdate }: Props) {
    const [showAddModal, setShowAddModal] = useState(false);

    const onCloseModal = () => setShowAddModal(false);

    const onUpdateModal = async (desc: string) => {
        message.loading({ content: 'Updating...', key: MessageKey });
        await onUpdate(desc);
        message.success({ content: 'Updated!', key: MessageKey, duration: 2 });
        onCloseModal();
    };

    return (
        <DescriptionContainer>
            <DescriptionText source={updatedDescription || description} />
            {onHover && (
                <EditIcon
                    twoToneColor="#52c41a"
                    onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setShowAddModal(true);
                    }}
                />
            )}
            {showAddModal && (
                <div
                    onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                    }}
                    aria-hidden="true"
                >
                    <UpdateDescriptionModal
                        title="Update description"
                        description={updatedDescription || description}
                        original={description}
                        onClose={onCloseModal}
                        onSubmit={onUpdateModal}
                    />
                </div>
            )}
            {updatedDescription && <EditedLabel>(edited)</EditedLabel>}
        </DescriptionContainer>
    );
}
