import { colors } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Editor } from '@src/alchemy-components/components/Editor/Editor';

const EditorContainer = styled.div`
    height: 300px;
    overflow: auto;
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
`;

type Props = {
    content: string | undefined;
    form: FormInstance;
};

const RichTextContent = ({ content, form }: Props) => {
    return (
        <Form form={form} initialValues={{ content }}>
            <Form.Item name="content">
                <EditorContainer>
                    <Editor
                        content={content}
                        placeholder="Write some text here..."
                        hideBorder
                        dataTestId="rich-text-documentation"
                        onChange={(newContent) => form.setFieldValue('content', newContent)}
                    />
                </EditorContainer>
            </Form.Item>
        </Form>
    );
};

export default RichTextContent;
