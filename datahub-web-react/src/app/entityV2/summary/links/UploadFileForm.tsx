import { Form } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { getFileNameFromUrl } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { FileNode } from '@components/components/FileNode/FileNode';

import { FileDragAndDropArea } from '@app/entityV2/shared/components/styled/LinkFormModal/FileDragAndDropArea';
import { LinkFormData, LinkFormVariant } from '@app/entityV2/summary/links/types';
import { FontSizeOptions } from '@components/theme/config';
import { useUploadFileHandler } from './useUploadFileHandler';

const StyledFileDragAndDropArea = styled(FileDragAndDropArea)``;

const FileWrapper = styled.div`
    margin-top: 16px;
`;

interface Props {
    initialValues?: Partial<LinkFormData>;
    fontSize?: FontSizeOptions;
}

export function UploadFileForm({ initialValues, fontSize: _fontSize }: Props) {
    const handleFileUpload = useUploadFileHandler();

    const [isFileUploading, setIsFileUploading] = useState<boolean>(false);
    const [fileName, setFileName] = useState<string>(getFileNameFromUrl(initialValues?.fileUrl ?? '') ?? '');

    const form = Form.useFormInstance<LinkFormData>();
    const variant = Form.useWatch('variant', form);
    const label = Form.useWatch('label', form);
    const fileUrl = Form.useWatch('fileUrl', form);

    const isRequired = useMemo(() => {
        return variant !== LinkFormVariant.URL;
    }, [variant]);

    const shouldShowDragAndDropArea = useMemo(() => !fileUrl && !isFileUploading, [fileUrl, isFileUploading]);

    const setLabel = useCallback(
        (newLabel: string) => {
            form.setFieldValue('label', newLabel);
            form.validateFields(['label']);
        },
        [form],
    );
    const setUrl = useCallback(
        (newFileUrl: string | null) => {
            form.setFieldValue('fileUrl', newFileUrl);
            form.validateFields(['fileUrl']);
        },
        [form],
    );

    const onFilesUpload = useCallback(
        async (files: File[]) => {
            const fileToUpload = files?.[0];
            if (!fileToUpload) return;

            setIsFileUploading(true);

            setFileName(fileToUpload.name);
            if (!label) setLabel(fileToUpload.name);

            const uploadedFileUrl = await handleFileUpload(fileToUpload);
            setUrl(uploadedFileUrl);
            setIsFileUploading(false);
        },
        [setLabel, setUrl, label, handleFileUpload],
    );

    const onFileRemove = useCallback(() => {
        setUrl('');
        setFileName('');
    }, [setUrl]);

    return (
        <FileWrapper>
            <Form.Item
                data-testid="link-form-modal-file-url"
                name="fileUrl"
                initialValue={initialValues?.fileUrl}
                rules={[
                    {
                        required: isRequired,
                        message: 'A file is required.',
                    },
                ]}
            >
                {shouldShowDragAndDropArea && <StyledFileDragAndDropArea onFilesUpload={onFilesUpload} />}
                {!shouldShowDragAndDropArea && (
                    <FileNode
                        fileName={fileName}
                        onClose={onFileRemove}
                        loading={isFileUploading}
                        size="md"
                        border
                    />
                )}
                <input type="text" hidden />
            </Form.Item>
        </FileWrapper>
    );
}
