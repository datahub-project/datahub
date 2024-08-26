import { Button, Form, Input, Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { SecretBuilderState } from './types';

const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';
const VALUE_FIELD_NAME = 'value';

type Props = {
    initialState?: SecretBuilderState;
    editSecret?: SecretBuilderState;
    visible: boolean;
    onSubmit?: (source: SecretBuilderState, resetState: () => void) => void;
    onUpdate?: (source: SecretBuilderState, resetState: () => void) => void;
    onCancel?: () => void;
};

export const SecretBuilderModal = ({ initialState, editSecret, visible, onSubmit, onUpdate, onCancel }: Props) => {
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();
    const { t } = useTranslation();

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

    const titleText = editSecret ? t('ingest.editSecret') : t('ingest.createANewSecret');

    return (
        <Modal
            width={540}
            title={<Typography.Text>{titleText}</Typography.Text>}
            visible={visible}
            onCancel={onCloseModal}
            zIndex={1051} // one higher than other modals - needed for managed ingestion forms
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button
                        data-testid="secret-modal-create-button"
                        id="createSecretButton"
                        onClick={() => {
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
                        }}
                        disabled={!createButtonEnabled}
                    >
                        {!editSecret ? t('common.create') : t('crud.update')}
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={initialState}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>{t('common.name')}</Typography.Text>}>
                    <Typography.Paragraph>
                         {t('ingest.secretNameDescription')}
                    </Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-name-input"
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: 'Enter a name.',
                            },
                            { whitespace: false },
                            { min: 1, max: 50 },
                            {
                                pattern: /^[a-zA-Z_]+[a-zA-Z0-9_]*$/,
                                message:
                                    'Please start the secret name with a letter, followed by letters, digits, or underscores only.',
                            },
                        ]}
                        hasFeedback
                    >
                        <Input placeholder={t('ingest.secretNameInputPlaceholder')} disabled={editSecret !== undefined} />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>{t('common.value')}</Typography.Text>}>
                    <Typography.Paragraph>
                    {t('ingest.secretValueDescription')}
                    </Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-value-input"
                        name={VALUE_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: 'Enter a value.',
                            },
                            // { whitespace: true },
                            { min: 1 },
                        ]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder={t('ingest.secretValueInputPlaceholder')} autoComplete="false" />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>{t('common.description')}</Typography.Text>}>
                    <Typography.Paragraph>
                    {t('ingest.secretDescriptionDescription')}
                    </Typography.Paragraph>
                    <Form.Item
                        data-testid="secret-modal-description-input"
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder={t('ingest.secretDescriptionInputPlaceholder')} />
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
};
