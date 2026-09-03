import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FormValues } from '@app/homeV3/modules/assetCollection/types';

const NameInput = styled(Form.Item)`
    margin-bottom: 16px;
`;

interface Props {
    form: FormInstance;
    formValues?: FormValues;
}

const ModuleDetailsForm = ({ form, formValues }: Props) => {
    const { t } = useTranslation('modules');
    return (
        <Form form={form} initialValues={formValues} autoComplete="off">
            <NameInput
                name="name"
                rules={[
                    {
                        required: true,
                        message: t('details.nameValidation'),
                    },
                ]}
            >
                <Input
                    label={t('details.nameLabel')}
                    placeholder={t('details.namePlaceholder')}
                    isRequired
                    data-testid="module-name"
                />
            </NameInput>
            {/* Should be used later, once support for description is added  */}
            {/* <Form.Item name="description">
                <TextArea label="Description" placeholder="Help others understand what this collection contains..." />
            </Form.Item> */}
        </Form>
    );
};

export default ModuleDetailsForm;
