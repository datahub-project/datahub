import React, { useEffect, useState } from 'react';
import { message, Button, Input, Modal, Typography, Form } from 'antd';
import { t } from 'i18next';
import { useTranslation } from 'react-i18next';
import { useUpdateCorpGroupPropertiesMutation } from '../../../graphql/group.generated';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';

type PropsData = {
    email: string | undefined;
    slack: string | undefined;
    urn: string | undefined;
    photoUrl: string | undefined;
};

type Props = {
    visible: boolean;
    onClose: () => void;
    onSave: () => void;
    editModalData: PropsData;
};
/** Regex Validations */
export const USER_NAME_REGEX = new RegExp('^[a-zA-Z ]*$');

export default function GroupEditModal({ visible, onClose, onSave, editModalData }: Props) {
    const { t } = useTranslation();
    const [updateCorpGroupPropertiesMutation] = useUpdateCorpGroupPropertiesMutation();
    const [form] = Form.useForm();

    const [saveButtonEnabled, setSaveButtonEnabled] = useState(true);
    const [data, setData] = useState<PropsData>({
        slack: editModalData.slack,
        email: editModalData.email,
        urn: editModalData.urn,
        photoUrl: editModalData.photoUrl,
    });

    useEffect(() => {
        setData({ ...editModalData });
    }, [editModalData]);

    // save changes function
    const onSaveChanges = () => {
        updateCorpGroupPropertiesMutation({
            variables: {
                urn: editModalData?.urn || '',
                input: {
                    email: data.email,
                    slack: data.slack,
                    pictureLink: data.photoUrl,
                },
            },
        })
            .then(() => {
                message.success({
                    content: t('common.saveChanges'),
                    duration: 3,
                });
                onSave(); // call the refetch function once save
                // clear the values from edit profile form
                setData({
                    email: '',
                    slack: '',
                    urn: '',
                    photoUrl: '',
                });
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `${t('crud.error.changesSaved')} \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#editGroupButton',
    });

    return (
        <Modal
            title= {t('user.editProfile')}
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button id="editGroupButton" onClick={onSaveChanges} disabled={saveButtonEnabled}>
                        {t('common.saveChanges')}
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={{ ...editModalData }}
                autoComplete="off"
                layout="vertical"
                onFieldsChange={() =>
                    setSaveButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item
                    name="email"
                    label={<Typography.Text strong>{t('common.email')}</Typography.Text>}
                    rules={[
                        {
                            type: 'email',
                            message: t('form.validEmailRequired'),
                        },
                        { whitespace: true },
                        { min: 2, max: 50 },
                    ]}
                    hasFeedback
                >
                    <Input
                        placeholder="engineering@example.com"
                        value={data.email}
                        onChange={(event) => setData({ ...data, email: event.target.value })}
                    />
                </Form.Item>
                <Form.Item
                    name="slack"
                    label={<Typography.Text strong>Slack Channel</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <Input
                        placeholder="#engineering"
                        value={data.slack}
                        onChange={(event) => setData({ ...data, slack: event.target.value })}
                    />
                </Form.Item>

                <Form.Item
                    name="photoUrl"
                    label={<Typography.Text strong>Image URL</Typography.Text>}
                    rules={[{ whitespace: true }, { type: 'url', message: 'not valid url' }]}
                    hasFeedback
                >
                    <Input
                        placeholder="https://www.example.com/photo.png"
                        value={data.photoUrl}
                        onChange={(event) => setData({ ...data, photoUrl: event.target.value })}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
}
