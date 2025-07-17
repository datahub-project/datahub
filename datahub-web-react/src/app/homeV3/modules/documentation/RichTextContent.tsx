import { Form, FormInstance } from 'antd';
import React from 'react';

import { Editor } from '@src/alchemy-components/components/Editor/Editor';

type Props = {
    content: string | undefined;
    form: FormInstance;
};

const RichTextContent = ({
    content,
    form,
}: Props) => {
    return (
        <Form form={form} initialValues={{ content }}>
            <Form.Item
                name="content"
                rules={[
                    {
                        required: true,
                        message: 'Please add content',
                    },
                ]}
            >
                <Editor content={content} />
            </Form.Item>
        </Form>
    );
};

export default RichTextContent;
