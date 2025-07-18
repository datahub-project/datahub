import { Input, TextArea } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';

import { LinkModuleParams } from '@types';

interface Props {
    form: FormInstance;
    formValues?: LinkModuleParams;
}

export default function LinkForm({ form, formValues }: Props) {
    return (
        <Form form={form} initialValues={formValues}>
            <Form.Item
                name="linkUrl"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the link URL',
                    },
                    {
                        type: 'url',
                        message: 'Please enter a valid URL',
                    },
                ]}
            >
                <Input label="Link" placeholder="www.datahub.com/events" isRequired />
            </Form.Item>
            <Form.Item name="imageUrl">
                <Input label="Image URL (Optional)" placeholder="Your image URL" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label="Description (Optional)" placeholder="Add description..." />
            </Form.Item>
        </Form>
    );
}
