/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
