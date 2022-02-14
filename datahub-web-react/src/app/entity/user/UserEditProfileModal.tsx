import React, { useState } from 'react';
import { message, Button, Input, Modal, Typography, Form } from 'antd';
import { useUpdateCorpUserPropertiesMutation } from '../../../graphql/user.generated';

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreate: () => void;
    data: {
        name: string | undefined;
        title: string | undefined;
        image: string | undefined;
        team: string | undefined;
        email: string | undefined;
        slack: string | undefined;
        phone: string | undefined;
        urn: string | undefined;
    };
};
/** Regex Validations */
export const validUserName = new RegExp('^[a-zA-Z ]*$');

export default function UserEditProfileModal({ visible, onClose, onCreate, data }: Props) {
    const [userName, setUserName] = useState('');
    const [userTitle, setUserTitle] = useState('');
    const [userImageURL, setImageURL] = useState('');
    const [userTeam, setUserTeam] = useState('');
    const [userEmail, setUserEmail] = useState('');
    const [userSlack, setUserSlack] = useState('');
    const [userPhoneNumber, setUserPhoneNumber] = useState('');
    const [updateCorpUserPropertiesMutation] = useUpdateCorpUserPropertiesMutation();

    const onCreateGroup = () => {
        console.log('inputs', userName, userTitle, userImageURL, userTeam, userEmail, userSlack);
        updateCorpUserPropertiesMutation({
            variables: {
                urn: data?.urn || '',
                input: {
                    displayName: userName || data.name,
                    title: userTitle || data.title,
                    pictureLink: userImageURL || data.image,
                    teams: userTeam.split(',') || data.team,
                    email: userEmail || data.email,
                    slack: userSlack || data.slack,
                    phone: userPhoneNumber || data.phone,
                },
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create group!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Created group!`,
                    duration: 3,
                });
                // onCreate(stagedName, stagedDescription);
                setUserName('');
                setUserTitle('');
                setImageURL('');
                setUserTeam('');
                setUserEmail('');
                setUserSlack('');
                setUserPhoneNumber('');
            });
        onClose();
        onCreate();
    };

    // userFieldValidation before submitting the changes.
    const userFieldValidations = () => {
        // TODO: add validations - need to ask Gabe
        if (userName !== '' || data.name) {
            return false;
        }
        return true;
    };

    return (
        <Modal
            title="Edit Profile"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={onCreateGroup} disabled={userFieldValidations()}>
                        Save Changes
                    </Button>
                </>
            }
        >
            <Form
                initialValues={{
                    name: data.name,
                    title: data.title,
                    image: data.image,
                    team: data.team,
                    email: data.email,
                    slack: data.slack,
                    phone: data.phone,
                }}
                autoComplete="off"
                layout="vertical"
            >
                <Form.Item
                    name="name"
                    label={<Typography.Text strong>Name</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: 'Please enter the User name.',
                        },
                        { whitespace: true },
                        { min: 2, max: 50 },
                        {
                            pattern: new RegExp('^[a-zA-Z ]*$'),
                            message: '',
                        },
                    ]}
                    hasFeedback
                >
                    <Input
                        placeholder="add name"
                        value={userName}
                        onChange={(event) => setUserName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item
                    name="title"
                    label={<Typography.Text strong>Title/Role</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <Input
                        placeholder="add title/role"
                        value={userTitle}
                        onChange={(event) => setUserTitle(event.target.value)}
                    />
                </Form.Item>
                <Form.Item
                    name="image"
                    label={<Typography.Text strong>Image URL</Typography.Text>}
                    rules={[{ whitespace: true }, { type: 'url', message: 'not valid url' }]}
                    hasFeedback
                >
                    <Input
                        placeholder="add image URL"
                        value={userImageURL}
                        onChange={(event) => setImageURL(event.target.value)}
                    />
                </Form.Item>
                <Form.Item
                    name="team"
                    label={<Typography.Text strong>Team</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                >
                    <Input
                        placeholder="add team name"
                        value={userTeam}
                        onChange={(event) => setUserTeam(event.target.value)}
                    />
                </Form.Item>
                <Form.Item
                    name="email"
                    label={<Typography.Text strong>Email</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: 'Please enter your email',
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
                        placeholder="add email"
                        value={userEmail}
                        onChange={(event) => setUserEmail(event.target.value)}
                    />
                </Form.Item>
                <Form.Item
                    name="slack"
                    label={<Typography.Text strong>Slack</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <Input
                        placeholder="add slack id"
                        value={userSlack}
                        onChange={(event) => setUserSlack(event.target.value)}
                    />
                </Form.Item>
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
                        placeholder="add phone number"
                        value={userPhoneNumber}
                        onChange={(event) => setUserPhoneNumber(event.target.value)}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
}
