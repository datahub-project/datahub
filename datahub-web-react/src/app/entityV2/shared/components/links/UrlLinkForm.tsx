import { Input } from '@components';
import { Form } from 'antd';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { LinkFormData, LinkFormVariant } from '@app/entityV2/shared/components/links/types';

const URL_PLACEHOLDER = 'https://';

export function UrlLinkForm() {
    const { t } = useTranslation('entity.shared.components');
    const form = Form.useFormInstance<LinkFormData>();
    const variant = Form.useWatch('variant', form);

    const isRequired = useMemo(() => {
        return variant !== LinkFormVariant.UploadFile;
    }, [variant]);

    return (
        <Form.Item
            name="url"
            rules={
                isRequired
                    ? [
                          {
                              required: true,
                              message: t('links.urlRequired'),
                          },
                          {
                              type: 'url',
                              message: t('links.urlInvalid'),
                          },
                      ]
                    : []
            }
        >
            <Input label="URL" placeholder={URL_PLACEHOLDER} inputTestId="url-input" isRequired={isRequired} />
        </Form.Item>
    );
}
