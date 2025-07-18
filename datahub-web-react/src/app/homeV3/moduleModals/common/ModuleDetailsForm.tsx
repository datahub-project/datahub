import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';

import { FormValues } from '@app/homeV3/modules/assetCollection/types';

interface Props {
    form: FormInstance;
    formValues?: FormValues;
}

const ModuleDetailsForm = ({ form, formValues }: Props) => {
    return (
        <Form form={form} initialValues={formValues}>
            <Form.Item
                name="name"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the name',
                    },
                ]}
            >
                <Input label="Name" placeholder="Choose a name for your widget" isRequired />
            </Form.Item>
            {/* Should be used later, once support for description is added  */}
            {/* <Form.Item name="description">
                <TextArea label="Description" placeholder="Help others understand what this collection contains..." />
            </Form.Item> */}
        </Form>
    );
};

export default ModuleDetailsForm;
