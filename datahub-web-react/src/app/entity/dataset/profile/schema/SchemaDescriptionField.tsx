import { Typography, Modal, Button, Input, message } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import { UpdateDatasetMutation } from '../../../../../graphql/dataset.generated';

const { TextArea } = Input;

const DescriptionText = styled(Typography.Text)`
    padding-right: 7px;
`;

const DescriptionTextInModal = styled(Typography.Text)`
    padding: 4px 11px;
`;

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    position: absolute;
    margin-top: 5px;
`;

const EditTextArea = styled(TextArea)`
    margin-top: 15px;
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
    const [updatedDesc, setDesc] = useState(updatedDescription || description);

    const onCloseModal = () => setShowAddModal(false);

    const onUpdateModal = async () => {
        message.loading({ content: 'Updating...', key: MessageKey });
        await onUpdate(updatedDesc);
        message.success({ content: 'Updated!', key: MessageKey, duration: 2 });
        onCloseModal();
    };

    return (
        <>
            <DescriptionText>{updatedDescription || description}</DescriptionText>
            {onHover && <EditIcon twoToneColor="#52c41a" onClick={() => setShowAddModal(true)} />}
            {showAddModal && (
                <Modal
                    title="Update description"
                    visible
                    onCancel={onCloseModal}
                    okButtonProps={{ disabled: !updatedDesc || updatedDesc.length === 0 }}
                    okText="Update"
                    footer={
                        <>
                            <Button onClick={onCloseModal}>Cancel</Button>
                            <Button
                                onClick={onUpdateModal}
                                disabled={
                                    !updatedDesc ||
                                    updatedDesc.length === 0 ||
                                    updatedDesc === (updatedDescription || description)
                                }
                            >
                                Update
                            </Button>
                        </>
                    }
                >
                    {(updatedDescription || description) && (
                        <DescriptionTextInModal>{updatedDescription || description}</DescriptionTextInModal>
                    )}
                    <EditTextArea
                        value={updatedDesc}
                        onChange={(e) => setDesc(e.target.value)}
                        placeholder="Description"
                        autoSize={{ minRows: 2, maxRows: 6 }}
                    />
                </Modal>
            )}
        </>
    );
}
