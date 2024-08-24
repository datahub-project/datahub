import React, { useEffect, useState } from 'react';
import { message, Button, Input, Modal, Typography, Form, Tooltip } from 'antd';
import { useUpdateCorpUserPropertiesMutation } from '../../../graphql/user.generated';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { useAppConfig } from '../../useAppConfig';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation();
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
                    content: t('crud.success.changesSaved'),
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
                message.error({ content: `${t('crud.error.changesSaved')}\n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#editUserButton',
    });

    return (
        <Modal
            title="Edit Profile"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button id="editUserButton" onClick={onSaveChanges} disabled={saveButtonEnabled}>
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
                    name="name"
                    label={<Typography.Text strong>{t('common.name')}</Typography.Text>}
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
                        placeholder="Analista de Dados"
                        value={data.title}
                        onChange={(event) => setData({ ...data, title: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Tooltip
                    title="A edição do URL da imagem foi desativada."
                    overlayStyle={readOnlyModeEnabled ? {} : { display: 'none' }}
                    placement="bottom"
                >
                    <Form.Item
                        name="image"
                        label={<Typography.Text strong>{t('post.imageUrl')}</Typography.Text>}
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
                    label={<Typography.Text strong>{t('common.team')}</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                >
                    <Input
                        placeholder="Engenharia de produto"
                        value={data.team}
                        onChange={(event) => setData({ ...data, team: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Form.Item
                    name="email"
                    label={<Typography.Text strong>{t('common.email')}</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: 'Digite seu e-mail',
                        },
                        {
                            type: 'email',
                            message: 'Por favor insira um e-mail válido',
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
                    label={<Typography.Text strong>Slack</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <Input
                        placeholder="john_smith"
                        value={data.slack}
                        onChange={(event) => setData({ ...data, slack: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Form.Item
                    name="phone"
                    label={<Typography.Text strong>{t('common.phone')}</Typography.Text>}
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
