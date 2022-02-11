import React, { useState } from 'react';
import { message, Modal, Button, Form, Input } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useGetAuthenticatedUser } from '../../../../useGetAuthenticatedUser';
import { useEntityData } from '../../EntityContext';
import { useAddLinkMutation } from '../../../../../graphql/mutations.generated';
import analytics, { EventType, EntityActionType } from '../../../../analytics';

type AddLinkProps = {
    buttonProps?: Record<string, unknown>;
    refetch?: () => Promise<any>;
};

export const AddLinkModal = ({ buttonProps, refetch }: AddLinkProps) => {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const user = useGetAuthenticatedUser();
    const { urn, entityType } = useEntityData();
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
        if (user?.corpUser.urn) {
            try {
                await addLinkMutation({
                    variables: { input: { linkUrl: formData.url, label: formData.label, resourceUrn: urn } },
                });
                message.success({ content: 'Link Added', duration: 2 });
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: urn,
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

    return (
        <>
            <Button icon={<PlusOutlined />} onClick={showModal} {...buttonProps}>
                Add Link
            </Button>
            <Modal
                title="Add Link"
                visible={isModalVisible}
                destroyOnClose
                onCancel={handleClose}
                footer={[
                    <Button type="text" onClick={handleClose}>
                        Cancel
                    </Button>,
                    <Button form="addLinkForm" key="submit" htmlType="submit">
                        Add
                    </Button>,
                ]}
            >
                <Form form={form} name="addLinkForm" onFinish={handleAdd} layout="vertical">
                    <Form.Item
                        name="url"
                        label="URL"
                        rules={[
                            {
                                required: true,
                                message: 'A URL is required.',
                            },
                            {
                                type: 'url',
                                message: 'This field must be a valid url.',
                            },
                        ]}
                    >
                        <Input placeholder="https://" autoFocus />
                    </Form.Item>
                    <Form.Item
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
