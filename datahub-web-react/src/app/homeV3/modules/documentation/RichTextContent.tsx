import { colors } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

import { UploadDownloadScenario } from '@types';

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
    const { urn: assetUrn } = useEntityData();
    const uploadFileAnalyticsCallbacks = useFileUploadAnalyticsCallbacks({
        scenario: UploadDownloadScenario.AssetDocumentation,
        assetUrn,
    });
    const { uploadFile } = useFileUpload({ scenario: UploadDownloadScenario.AssetDocumentation, assetUrn });

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
                        uploadFileProps={
                            assetUrn // only support file upload on profile pages for now due to permissions
                                ? {
                                      onFileUpload: uploadFile,
                                      ...uploadFileAnalyticsCallbacks,
                                  }
                                : undefined
                        }
                    />
                </EditorContainer>
            </Form.Item>
        </Form>
    );
};

export default RichTextContent;
