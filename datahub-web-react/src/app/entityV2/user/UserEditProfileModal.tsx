import { MoreOutlined } from '@ant-design/icons';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { message, Button, Input, Modal, Typography, Form } from 'antd';
import { Tooltip } from '@components';
import { useUpdateCorpUserPropertiesMutation } from '../../../graphql/user.generated';
import { useAppConfig } from '../../useAppConfig';

const StyledInput = styled(Input)`
    margin-bottom: 20px;
`;

const StyledText = styled(Typography.Text)`
    display: block;
    position: absolute;
    bottom: 11.5rem;
`;

type PropsData = {
    name: string | undefined;
    title: string | undefined;
    image: string | undefined;
    team: string | undefined;
    email: string | undefined;
    slack: string | undefined;
    phone: string | undefined;
    urn: string | undefined;
};

type Props = {
    visible: boolean;
    onClose: () => void;
    onSave: () => void;
    editModalData: PropsData;
};
/** Regex Validations */
export const USER_NAME_REGEX = new RegExp('^[a-zA-Z ]*$');

export default function UserEditProfileModal({ visible, onClose, onSave, editModalData }: Props) {
    const { config } = useAppConfig();
    const { readOnlyModeEnabled } = config.featureFlags;
    const [updateCorpUserPropertiesMutation] = useUpdateCorpUserPropertiesMutation();
    const [form] = Form.useForm();

    const [saveButtonEnabled, setSaveButtonEnabled] = useState(true);
    const [data, setData] = useState<PropsData>({
        name: editModalData.name,
        title: editModalData.title,
        image: editModalData.image,
        team: editModalData.team,
        email: editModalData.email,
        slack: editModalData.slack,
        phone: editModalData.phone,
        urn: editModalData.urn,
    });

    useEffect(() => {
        setData({ ...editModalData });
    }, [editModalData]);

    // save changes function
    const onSaveChanges = () => {
        updateCorpUserPropertiesMutation({
            variables: {
                urn: editModalData?.urn || '',
                input: {
                    displayName: data.name,
                    title: data.title,
                    pictureLink: data.image,
                    teams: data.team?.split(','),
                    email: data.email,
                    slack: data.slack,
                    phone: data.phone,
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
                    name: '',
                    title: '',
                    image: '',
                    team: '',
                    email: '',
                    slack: '',
                    phone: '',
                    urn: '',
                });
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to Save changes!: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    };

    return (
        <Modal
            title="Edit Profile"
            open={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button type="primary" id="editUserButton" onClick={onSaveChanges} disabled={saveButtonEnabled}>
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
                onKeyPress={(event) => {
                    if (event.key === 'Enter') {
                        event.preventDefault();
                        onSaveChanges();
                    }
                }}
            >
                <Form.Item
                    name="name"
                    label={<Typography.Text strong>Name</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: 'Enter a display name.',
                        },
                        { whitespace: true },
                        { min: 2, max: 50 },
                        {
                            pattern: USER_NAME_REGEX,
                            message: '',
                        },
                    ]}
                    hasFeedback
                >
                    <Input
                        placeholder="John Smith"
                        value={data.name}
                        onChange={(event) => setData({ ...data, name: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Form.Item
                    name="title"
                    label={<Typography.Text strong>Title/Role</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <Input
                        placeholder="Data Analyst"
                        value={data.title}
                        onChange={(event) => setData({ ...data, title: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Tooltip
                    title="Editing image URL has been disabled."
                    overlayStyle={readOnlyModeEnabled ? {} : { display: 'none' }}
                    placement="bottom"
                >
                    <Form.Item
                        name="image"
                        label={<Typography.Text strong>Image URL</Typography.Text>}
                        rules={[{ whitespace: true }, { type: 'url', message: 'not valid url' }]}
                        hasFeedback
                    >
                        <Input
                            placeholder="https://www.example.com/photo.png"
                            value={data.image}
                            onChange={(event) => setData({ ...data, image: event.target.value })}
                            disabled={readOnlyModeEnabled}
                        />
                    </Form.Item>
                </Tooltip>
                <Form.Item
                    name="team"
                    label={<Typography.Text strong>Team</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                >
                    <Input
                        placeholder="Product Engineering"
                        value={data.team}
                        onChange={(event) => setData({ ...data, team: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Form.Item
                    name="email"
                    label={<Typography.Text strong>Email</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: 'Enter your email',
                        },
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
                        placeholder="john.smith@example.com"
                        value={data.email}
                        onChange={(event) => setData({ ...data, email: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Form.Item
                    name="slack"
                    label={
                        <>
                            <Typography.Text strong>Slack Member ID</Typography.Text>
                        </>
                    }
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <StyledInput
                        placeholder="ABC12345678"
                        value={data.slack}
                        onChange={(event) => setData({ ...data, slack: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <StyledText type="secondary">
                    Find your member ID from the <MoreOutlined /> menu in your Slack profile. More info{' '}
                    <a
                        href="https://slack.com/intl/en-ca/help/articles/212906697-Where-can-I-find-my-Slack-member-ID-"
                        target="_blank"
                        rel="noreferrer"
                    >
                        here.
                    </a>
                </StyledText>
                <Form.Item
                    name="phone"
                    label={<Typography.Text strong>Phone</Typography.Text>}
                    rules={[
                        {
                            pattern: new RegExp('^(?=.*[0-9])[- +()0-9]+$'),
                            message: 'not valid phone number',
                        },
                        {
                            min: 5,
                            max: 15,
                        },
                    ]}
                    hasFeedback
                >
                    <Input
                        placeholder="444-999-9999"
                        value={data.phone}
                        onChange={(event) => setData({ ...data, phone: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
}
