import { Form, Input, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { SecretBuilderState } from '@app/ingestV2/secret/types';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Modal } from '@src/alchemy-components';

const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';
const VALUE_FIELD_NAME = 'value';

type Props = {
    initialState?: SecretBuilderState;
    editSecret?: SecretBuilderState;
    open: boolean;
    onSubmit?: (source: SecretBuilderState, resetState: () => void) => void;
    onUpdate?: (source: SecretBuilderState, resetState: () => void) => void;
    onCancel?: () => void;
};

export const SecretBuilderModal = ({ initialState, editSecret, open, onSubmit, onUpdate, onCancel }: Props) => {
    const { t } = useTranslation('ingestion');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createSecretButton',
    });

    useEffect(() => {
        if (editSecret) {
            form.setFieldsValue({
                name: editSecret.name,
                description: editSecret.description,
                value: editSecret.value,
            });
        }
    }, [editSecret, form]);

    function resetValues() {
        setCreateButtonEnabled(false);
        form.resetFields();
    }

    const onCloseModal = () => {
        setCreateButtonEnabled(false);
        form.resetFields();
        onCancel?.();
    };

    const titleText = editSecret ? t('secret.editTitle') : t('secret.createTitle');

    return (
        <Modal
            width={540}
            title={titleText}
            open={open}
            onCancel={onCloseModal}
            zIndex={1051} // one higher than other modals - needed for managed ingestion forms
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onCloseModal,
                },
                {
                    text: !editSecret ? tc('create') : tc('update'),
                    variant: 'filled',
                    buttonDataTestId: 'secret-modal-create-button',
                    id: 'createSecretButton',
                    onClick: () => {
                        if (!editSecret) {
                            onSubmit?.(
                                {
                                    name: form.getFieldValue(NAME_FIELD_NAME),
                                    description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                                    value: form.getFieldValue(VALUE_FIELD_NAME),
                                },
                                resetValues,
                            );
                        } else {
                            onUpdate?.(
                                {
                                    urn: editSecret?.urn,
                                    name: form.getFieldValue(NAME_FIELD_NAME),
                                    description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                                    value: form.getFieldValue(VALUE_FIELD_NAME),
                                },
                                resetValues,
                            );
                        }
                    },
                    disabled: !createButtonEnabled,
                },
            ]}
        >
            <Form
                form={form}
                initialValues={initialState}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>{tl('name')}</Typography.Text>}>
                    <Typography.Paragraph>{t('secret.nameHelp')}</Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-name-input"
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: t('secret.nameRequired'),
                            },
                            { whitespace: false },
                            { min: 1, max: 50 },
                            {
                                pattern: /^[a-zA-Z_]+[a-zA-Z0-9_]*$/,
                                message: t('secret.namePattern'),
                            },
                        ]}
                        hasFeedback
                    >
                        <Input placeholder={t('secret.namePlaceholder')} disabled={editSecret !== undefined} />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>{tl('value')}</Typography.Text>}>
                    <Typography.Paragraph>{t('secret.valueHelp')}</Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-value-input"
                        name={VALUE_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: t('secret.valueRequired'),
                            },
                            // { whitespace: true },
                            { min: 1 },
                        ]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder={t('secret.valuePlaceholder')} autoComplete="false" />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>{tl('description')}</Typography.Text>}>
                    <Typography.Paragraph>{t('secret.descriptionHelp')}</Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-description-input"
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder={t('secret.descriptionPlaceholder')} />
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
};
