import { MoreOutlined } from '@ant-design/icons';
import { Modal, Tooltip } from '@components';
import { Form, Input, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useAppConfig } from '@app/useAppConfig';

import { useUpdateCorpUserPropertiesMutation } from '@graphql/user.generated';

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
const USER_NAME_REGEX = new RegExp('^[a-zA-Z ]*$');
const PHONE_REGEX = new RegExp('^(?=.*[0-9])[- +()0-9]+$');

export default function UserEditProfileModal({ visible, onClose, onSave, editModalData }: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const { t: tl } = useTranslation('common.labels');
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
                    content: tf('changesSaved'),
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
                onClose();
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('shared.saveChangesError', { error: e.message || '' }), duration: 3 });
                // Reset form state to original values so the rejected input is discarded
                setData({ ...editModalData });
                form.setFieldsValue({ ...editModalData });
            });
    };

    return (
        <Modal
            title={t('shared.editProfileTitle')}
            open={visible}
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('saveChanges'),
                    variant: 'filled',
                    id: 'editUserButton',
                    disabled: saveButtonEnabled,
                    onClick: onSaveChanges,
                },
            ]}
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
                    label={<Typography.Text strong>{tl('name')}</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: t('user.enterDisplayNameError'),
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
                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
                        placeholder="John Smith"
                        value={data.name}
                        onChange={(event) => setData({ ...data, name: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Form.Item
                    name="title"
                    label={<Typography.Text strong>{t('user.titleRoleLabel')}</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <Input
                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
                        placeholder="Data Analyst"
                        value={data.title}
                        onChange={(event) => setData({ ...data, title: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Tooltip
                    title={t('user.editImageUrlDisabledTooltip')}
                    overlayStyle={readOnlyModeEnabled ? {} : { display: 'none' }}
                    placement="bottom"
                >
                    <Form.Item
                        name="image"
                        label={<Typography.Text strong>{t('user.imageUrlLabel')}</Typography.Text>}
                        rules={[{ whitespace: true }, { type: 'url', message: t('user.invalidUrlError') }]}
                        hasFeedback
                    >
                        <Input
                            // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
                            placeholder="https://www.example.com/photo.png"
                            value={data.image}
                            onChange={(event) => setData({ ...data, image: event.target.value })}
                            disabled={readOnlyModeEnabled}
                        />
                    </Form.Item>
                </Tooltip>
                <Form.Item
                    name="team"
                    label={<Typography.Text strong>{t('user.teamLabel')}</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                >
                    <Input
                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
                        placeholder="Product Engineering"
                        value={data.team}
                        onChange={(event) => setData({ ...data, team: event.target.value })}
                        disabled={readOnlyModeEnabled}
                    />
                </Form.Item>
                <Form.Item
                    name="email"
                    label={<Typography.Text strong>{t('shared.emailLabel')}</Typography.Text>}
                    rules={[
                        {
                            required: true,
                            message: t('user.enterEmailError'),
                        },
                        {
                            type: 'email',
                            message: t('shared.invalidEmailError'),
                        },
                        { whitespace: true },
                        { min: 2, max: 50 },
                    ]}
                    hasFeedback
                >
                    <Input
                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
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
                            <Typography.Text strong>{t('user.slackMemberIdLabel')}</Typography.Text>
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
                    <Trans
                        t={t}
                        i18nKey="user.slackMemberIdHelp"
                        components={{
                            icon: <MoreOutlined />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    href="https://slack.com/intl/en-ca/help/articles/212906697-Where-can-I-find-my-Slack-member-ID-"
                                    target="_blank"
                                    rel="noreferrer"
                                />
                            ),
                        }}
                    />
                </StyledText>
                <Form.Item
                    name="phone"
                    label={<Typography.Text strong>{t('shared.phoneLabel')}</Typography.Text>}
                    rules={[
                        {
                            pattern: PHONE_REGEX,
                            message: t('user.invalidPhoneError'),
                        },
                        {
                            min: 5,
                            max: 15,
                        },
                    ]}
                    hasFeedback
                >
                    <Input
                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
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
