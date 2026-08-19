import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

import { UploadDownloadScenario } from '@types';

const EditorContainer = styled.div`
    height: 300px;
    overflow: auto;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
`;

const CONTENT_FIELD_NAME = 'content';

type Props = {
    content: string | undefined;
    form: FormInstance;
};

const RichTextContent = ({ content, form }: Props) => {
    const { t } = useTranslation('modules');
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
                        placeholder={t('documentation.contentPlaceholder')}
                        hideBorder
                        dataTestId="rich-text-documentation"
                        onChange={(newContent) => form.setFieldValue(CONTENT_FIELD_NAME, newContent)}
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
