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
    const { t } = useTranslation('modules');
    return (
        <Form form={form} initialValues={formValues}>
            <Form.Item
                name="linkUrl"
                rules={[
                    {
                        required: true,
                        message: t('link.urlValidation'),
                    },
                    {
                        type: 'url',
                        message: t('link.urlValidationInvalid'),
                    },
                ]}
            >
                <Input label={t('link.urlLabel')} placeholder={URL_PLACEHOLDER} isRequired data-testid="link-url" />
            </Form.Item>
            <Form.Item
                name="imageUrl"
                rules={[
                    {
                        type: 'url',
                        message: t('link.imageUrlValidation'),
                    },
                ]}
            >
                <Input label={t('link.imageUrlLabel')} placeholder={t('link.imageUrlPlaceholder')} />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label={t('link.descriptionLabel')} placeholder={t('link.descriptionPlaceholder')} />
            </Form.Item>
        </Form>
    );
}
