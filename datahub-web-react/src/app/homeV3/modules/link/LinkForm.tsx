import { Input, TextArea } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { LinkModuleParams } from '@types';

interface Props {
    form: FormInstance;
    formValues?: LinkModuleParams;
}

const URL_PLACEHOLDER = 'https://www.datahub.com';

export default function LinkForm({ form, formValues }: Props) {
    const { t } = useTranslation('module.link');
    return (
        <Form form={form} initialValues={formValues}>
            <Form.Item
                name="linkUrl"
                rules={[
                    {
                        required: true,
                        message: t('urlValidation'),
                    },
                    {
                        type: 'url',
                        message: t('urlValidationInvalid'),
                    },
                ]}
            >
                <Input label={t('urlLabel')} placeholder={URL_PLACEHOLDER} isRequired data-testid="link-url" />
            </Form.Item>
            <Form.Item
                name="imageUrl"
                rules={[
                    {
                        type: 'url',
                        message: t('imageUrlValidation'),
                    },
                ]}
            >
                <Input label={t('imageUrlLabel')} placeholder={t('imageUrlPlaceholder')} />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label={t('descriptionLabel')} placeholder={t('descriptionPlaceholder')} />
            </Form.Item>
        </Form>
    );
}
