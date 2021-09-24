import React, { useState } from 'react';
import { message, Modal, Button, Form, Input } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useGetAuthenticatedUser } from '../../../../useGetAuthenticatedUser';

import { GenericEntityUpdate } from '../../types';
import { useEntityData, useEntityUpdate } from '../../EntityContext';

export const AddLinkModal = ({ buttonProps }: { buttonProps?: Record<string, unknown> }) => {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const user = useGetAuthenticatedUser();
    const { urn, entityData } = useEntityData();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();

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
            const links = entityData?.institutionalMemory?.elements || [];

            const newLinks = links.map((link) => {
                return {
                    author: link.author.urn,
                    url: link.url,
                    description: link.description,
                    createdAt: link.created.time,
                };
            });

            newLinks.push({
                author: user?.corpUser.urn,
                createdAt: Date.now(),
                ...formData,
            });

            try {
                await updateEntity({
                    variables: { input: { urn, institutionalMemory: { elements: newLinks } } },
                });
                message.success({ content: 'Link Added', duration: 2 });
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to add link: \n ${e.message || ''}`, duration: 3 });
                }
            }

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
                        name="description"
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
