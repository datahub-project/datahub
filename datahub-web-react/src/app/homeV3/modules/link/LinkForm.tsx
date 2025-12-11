/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
                <Input label="Link" placeholder="https://www.datahub.com" isRequired data-testid="link-url" />
            </Form.Item>
            <Form.Item
                name="imageUrl"
                rules={[
                    {
                        type: 'url',
                        message: 'Please enter a valid URL',
                    },
                ]}
            >
                <Input label="Image URL (Optional)" placeholder="Your image URL" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label="Description (Optional)" placeholder="Add description..." />
            </Form.Item>
        </Form>
    );
}
