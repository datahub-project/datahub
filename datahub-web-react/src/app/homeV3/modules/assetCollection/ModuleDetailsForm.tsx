import { Input, TextArea } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';

import { ModuleInfo } from '@app/homeV3/modules/types';

interface Props {
    form: FormInstance;
    formValues?: Partial<ModuleInfo>;
}

const ModuleDetailsForm: React.FC<Props> = ({ form, formValues }) => {
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
            <Form.Item name="description">
                <TextArea label="Description" placeholder="Help others understand what this collection contains..." />
            </Form.Item>
        </Form>
    );
};

export default ModuleDetailsForm;
