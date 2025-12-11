/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PictureOutlined } from '@ant-design/icons';
import { useCommands } from '@remirror/react';
import { Form, Input, Modal, Typography } from 'antd';
import React, { useState } from 'react';

import { CommandButton } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/CommandButton';

export const AddImageButton = () => {
    const [isModalVisible, setModalVisible] = useState(false);
    const [form] = Form.useForm();
    const { insertImage } = useCommands();

    const handleButtonClick = () => {
        setModalVisible(true);
    };

    const handleOk = () => {
        form.validateFields()
            .then((values) => {
                form.resetFields();
                setModalVisible(false);
                insertImage(values);
            })
            .catch((info) => {
                console.log('Validate Failed:', info);
            });
    };

    const handleCancel = () => {
        setModalVisible(false);
    };

    return (
        <>
            <CommandButton
                active={false}
                icon={<PictureOutlined />}
                commandName="insertImage"
                onClick={handleButtonClick}
            />
            <Modal title="Add Image" open={isModalVisible} okText="Save" onOk={handleOk} onCancel={handleCancel}>
                <Form form={form} layout="vertical" colon={false} requiredMark={false}>
                    <Form.Item
                        name="src"
                        label={<Typography.Text strong>Image URL</Typography.Text>}
                        rules={[{ required: true }]}
                    >
                        <Input placeholder="http://www.example.com/image.jpg" autoFocus />
                    </Form.Item>
                    <Form.Item name="alt" label={<Typography.Text strong>Alt Text</Typography.Text>}>
                        <Input />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
