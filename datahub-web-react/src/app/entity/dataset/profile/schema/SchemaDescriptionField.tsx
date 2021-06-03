import { Typography, message } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import { UpdateDatasetMutation } from '../../../../../graphql/dataset.generated';
import UpdateDescriptionModal from '../modal/UpdateDescriptionModal';
import MarkdownViewer from '../../../shared/MarkdownViewer';

const DescriptionContainer = styled.div<{ hover?: string }>`
    position: relative;
    display: flex;
    flex-direction: row;
    ${(props) => (props.hover ? '' : 'padding-right: 19px;')}
`;

const DescriptionText = styled(MarkdownViewer)`
    padding-right: 8px;
`;

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    margin-top: 5px;
    margin-left: 5px;
`;

const EditedLabel = styled(Typography.Text)`
    position: absolute;
    right: -10px;
    top: -15px;
    color: rgba(150, 150, 150, 0.5);
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
        <DescriptionContainer
            onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
            }}
            hover={onHover ? 'true' : undefined}
        >
            <DescriptionText source={updatedDescription || description} />
            {onHover && <EditIcon twoToneColor="#52c41a" onClick={() => setShowAddModal(true)} />}
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
