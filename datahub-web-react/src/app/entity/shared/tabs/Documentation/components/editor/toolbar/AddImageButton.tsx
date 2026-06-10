import { PictureOutlined } from '@ant-design/icons';
import { useCommands } from '@remirror/react';
import { Form, Input, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { CommandButton } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/CommandButton';

const IMAGE_URL_PLACEHOLDER = 'http://www.example.com/image.jpg';

export const AddImageButton = () => {
    const { t } = useTranslation('entity.profile.editor');
    const { t: tc } = useTranslation('common.actions');
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
            <Modal
                title={t('addImage.title')}
                open={isModalVisible}
                okText={tc('save')}
                onOk={handleOk}
                onCancel={handleCancel}
            >
                <Form form={form} layout="vertical" colon={false} requiredMark={false}>
                    <Form.Item
                        name="src"
                        label={<Typography.Text strong>{t('addImage.imageUrl')}</Typography.Text>}
                        rules={[{ required: true }]}
                    >
                        <Input placeholder={IMAGE_URL_PLACEHOLDER} autoFocus />
                    </Form.Item>
                    <Form.Item name="alt" label={<Typography.Text strong>{t('addImage.altText')}</Typography.Text>}>
                        <Input />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
