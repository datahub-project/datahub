import { Button, Form, Input, Modal, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';

import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';

import { useUpdateCorpGroupPropertiesMutation } from '@graphql/group.generated';

type PropsData = {
    email: string | undefined;
    slack: string | undefined;
    urn: string | undefined;
    photoUrl: string | undefined;
};

type Props = {
    open: boolean;
    onClose: () => void;
    onSave: () => void;
    editModalData: PropsData;
};
/** Regex Validations */
export const USER_NAME_REGEX = new RegExp('^[a-zA-Z ]*$');

export default function GroupEditModal({ open, onClose, onSave, editModalData }: Props) {
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
                    content: `Changes saved.`,
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
                message.error({ content: `Failed to Save changes!: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#editGroupButton',
    });

    return (
        <Modal
            title="Edit Profile"
            open={open}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="editGroupButton" onClick={onSaveChanges} disabled={saveButtonEnabled}>
                        Save Changes
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
                    label={<Typography.Text strong>Email</Typography.Text>}
                    rules={[
                        {
                            type: 'email',
                            message: 'Please enter valid email',
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
