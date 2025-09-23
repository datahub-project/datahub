import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
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
    return (
        <Form form={form} initialValues={formValues} autoComplete="off">
            <NameInput
                name="name"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the name',
                    },
                ]}
            >
                <Input label="Name" placeholder="Choose a name for your module" isRequired data-testid="module-name" />
            </NameInput>
            {/* Should be used later, once support for description is added  */}
            {/* <Form.Item name="description">
                <TextArea label="Description" placeholder="Help others understand what this collection contains..." />
            </Form.Item> */}
        </Form>
    );
};

export default ModuleDetailsForm;
