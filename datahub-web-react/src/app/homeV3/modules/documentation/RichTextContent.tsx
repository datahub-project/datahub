import { Form, FormInstance } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Editor } from '@src/alchemy-components/components/Editor/Editor';

const StyledEditor = styled(Editor)`
    height: 300px;
    overflow: auto;
`;

const toolbarStyles = {
    width: '100%',
    justifyContent: 'flex-start',
};

type Props = {
    content: string | undefined;
    form: FormInstance;
};

const RichTextContent = ({ content, form }: Props) => {
    return (
        <Form form={form} initialValues={{ content }}>
            <Form.Item name="content" data-testid="rich-text-documentation">
                <StyledEditor content={content} placeholder="Write some text here..." toolbarStyles={toolbarStyles} />
            </Form.Item>
        </Form>
    );
};

export default RichTextContent;
