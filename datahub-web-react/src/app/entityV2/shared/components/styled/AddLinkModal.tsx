import React, { useState } from 'react';
import { Button as AntButton, message, Modal, Form, Input } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { useEntityData, useMutationUrn } from '../../../../entity/shared/EntityContext';
import { useAddLinkMutation } from '../../../../../graphql/mutations.generated';
import analytics, { EventType, EntityActionType } from '../../../../analytics';
import { useUserContext } from '../../../../context/useUserContext';

type AddLinkProps = {
    buttonProps?: Record<string, unknown>;
    refetch?: () => Promise<any>;
    buttonType?: string;
};

export const AddLinkModal = ({ buttonProps, refetch, buttonType }: AddLinkProps) => {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const mutationUrn = useMutationUrn();
    const user = useUserContext();
    const { entityType } = useEntityData();
    const [addLinkMutation] = useAddLinkMutation();

    const [form] = Form.useForm();

    const showModal = () => {
        setIsModalVisible(true);
    };

    const handleClose = () => {
        form.resetFields();
        setIsModalVisible(false);
    };

    const handleAdd = async (formData: any) => {
        if (user?.urn) {
            try {
                await addLinkMutation({
                    variables: { input: { linkUrl: formData.url, label: formData.label, resourceUrn: mutationUrn } },
                });
                message.success({ content: 'Link Added', duration: 2 });
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: mutationUrn,
                    actionType: EntityActionType.UpdateLinks,
                });
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to add link: \n ${e.message || ''}`, duration: 3 });
                }
            }
            refetch?.();
            handleClose();
        } else {
            message.error({ content: `Error adding link: no user`, duration: 2 });
        }
    };

    const renderButton = (bType: string | undefined) => {
        if (bType === 'transparent') {
            return (
                <Button data-testid="add-link-button" variant="outline" onClick={showModal} {...buttonProps}>
                    <PlusOutlined />
                    Add Link
                </Button>
            );
        }
        if (bType === 'text') {
            return (
                <AntButton data-testid="add-link-button" onClick={showModal} type="text">
                    <PlusOutlined />
                    Add Link
                </AntButton>
            );
        }
        return (
            <Button variant="outline" data-testid="add-link-button" onClick={showModal} {...buttonProps}>
                <PlusOutlined />
                Add Link
            </Button>
        );
    };

    return (
        <>
            {renderButton(buttonType)}
            <Modal
                title="Add Link"
                visible={isModalVisible}
                destroyOnClose
                onCancel={handleClose}
                footer={[
                    <ModalButtonContainer>
                        <Button variant="text" onClick={handleClose}>
                            Cancel
                        </Button>
                        <Button data-testid="add-link-modal-add-button" form="addLinkForm" key="submit">
                            Add
                        </Button>
                    </ModalButtonContainer>,
                ]}
            >
                <Form form={form} name="addLinkForm" onFinish={handleAdd} layout="vertical">
                    <Form.Item
                        data-testid="add-link-modal-url"
                        name="url"
                        label="URL"
                        rules={[
                            {
                                required: true,
                                message: 'A URL is required.',
                            },
                            {
                                type: 'url',
                                warningOnly: true,
                                message: 'This field must be a valid url.',
                            },
                        ]}
                    >
                        <Input placeholder="https://" autoFocus />
                    </Form.Item>
                    <Form.Item
                        data-testid="add-link-modal-label"
                        name="label"
                        label="Label"
                        rules={[
                            {
                                required: true,
                                message: 'A label is required.',
                            },
                        ]}
                    >
                        <Input placeholder="A short label for this link" />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
