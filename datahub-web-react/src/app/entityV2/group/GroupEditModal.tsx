import { Modal } from '@components';
import { Form, Input, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useUpdateCorpGroupPropertiesMutation } from '@graphql/group.generated';

type PropsData = {
    name: string | undefined;
    email: string | undefined;
    slack: string | undefined;
    urn: string | undefined;
};

type Props = {
    canEditGroupName?: boolean;
    visible: boolean;
    onClose: () => void;
    onSave: () => void;
    handleTitleUpdate: (name: string) => void;
    editModalData: PropsData;
    updateName?: (name: string) => void; // TODO: Add name to the update mutation for groups to avoid 2 calls.
};

export default function GroupEditModal({
    canEditGroupName,
    visible,
    onClose,
    onSave,
    editModalData,
    handleTitleUpdate,
    updateName,
}: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const { t: tl } = useTranslation('common.labels');
    const [updateCorpGroupPropertiesMutation] = useUpdateCorpGroupPropertiesMutation();
    const [form] = Form.useForm();

    console.log(updateName); // will used later now to fix lint added in console

    const [saveButtonEnabled, setSaveButtonEnabled] = useState(true);
    const [data, setData] = useState<PropsData>({
        name: editModalData.name,
        slack: editModalData.slack,
        email: editModalData.email,
        urn: editModalData.urn,
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
                    email: '',
                    slack: '',
                    urn: '',
                });
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('shared.saveChangesError', { error: e.message || '' }), duration: 3 });
            });
        handleTitleUpdate(data?.name || '');
        onClose();
    };

    return (
        <Modal
            title={t('shared.editProfileTitle')}
            open={visible}
            onCancel={onClose}
            dataTestId="group-edit-profile-modal"
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('saveChanges'),
                    onClick: onSaveChanges,
                    variant: 'filled',
                    disabled: saveButtonEnabled,
                    id: 'editGroupButton',
                    buttonDataTestId: 'group-edit-profile-save-button',
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
                {canEditGroupName && (
                    <Form.Item name="name" label={<Typography.Text strong>{tl('name')}</Typography.Text>}>
                        <Input
                            data-testid="group-profile-name-input"
                            value={data.name}
                            onChange={(event) => setData({ ...data, name: event.target.value })}
                        />
                    </Form.Item>
                )}
                <Form.Item
                    name="email"
                    label={<Typography.Text strong>{t('shared.emailLabel')}</Typography.Text>}
                    rules={[
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
                        data-testid="group-profile-email-input"
                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
                        placeholder="engineering@example.com"
                        value={data.email}
                        onChange={(event) => setData({ ...data, email: event.target.value })}
                    />
                </Form.Item>
                <Form.Item
                    name="slack"
                    label={<Typography.Text strong>{t('group.slackChannelLabel')}</Typography.Text>}
                    rules={[{ whitespace: true }, { min: 2, max: 50 }]}
                    hasFeedback
                >
                    <Input
                        data-testid="group-profile-slack-input"
                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) example-value placeholder, intentionally English
                        placeholder="#engineering"
                        value={data.slack}
                        onChange={(event) => setData({ ...data, slack: event.target.value })}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
}
