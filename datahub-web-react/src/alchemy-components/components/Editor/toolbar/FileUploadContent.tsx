import { Button, Text, notification } from '@components';
import { useRemirrorContext } from '@remirror/react';
import React, { useRef } from 'react';
import styled from 'styled-components';

import {
    FileDragDropExtension,
    SUPPORTED_FILE_TYPES,
    createFileNodeAttributes,
    validateFile,
} from '@components/components/Editor/extensions/fileDragDrop';
import { FileUploadFailureType } from '@components/components/Editor/types';

const ContentWrapper = styled.div`
    gap: 8px;
    display: flex;
    flex-direction: column;
`;

const StyledText = styled(Text)`
    text-align: center;
`;

const StyledButton = styled(Button)`
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    text-align: center;
`;

const FileInput = styled.input`
    display: none;
`;

interface Props {
    hideDropdown: () => void;
}

export const FileUploadContent = ({ hideDropdown }: Props) => {
    const { commands } = useRemirrorContext();

    const fileInputRef = useRef<HTMLInputElement>(null);
    const remirrorContext = useRemirrorContext();
    const fileExtension = remirrorContext.getExtension(FileDragDropExtension);

    const handlebuttonClick = () => {
        fileInputRef.current?.click();
    };

    const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const input = event.target as HTMLInputElement;
        const files = input.files ? Array.from(input.files) : [];
        if (files.length === 0) return;

        const supportedTypes = SUPPORTED_FILE_TYPES;
        const { onFileUpload, onFileUploadAttempt, onFileUploadFailed, onFileUploadSucceeded } = fileExtension.options;

        try {
            // Process files concurrently
            await Promise.all(
                files.map(async (file) => {
                    onFileUploadAttempt?.(file.type, file.size, 'button');

                    const validation = validateFile(file, { allowedTypes: supportedTypes });
                    if (!validation.isValid) {
                        console.error(validation.error);
                        onFileUploadFailed?.(
                            file.type,
                            file.size,
                            'button',
                            validation.failureType || FileUploadFailureType.UNKNOWN,
                        );
                        notification.error({
                            message: 'Upload Failed',
                            description: validation.displayError || validation.error,
                        });
                        return; // Skip invalid files
                    }

                    // Create placeholder node
                    const attrs = createFileNodeAttributes(file);
                    commands.insertFileNode({ ...attrs, url: '' });

                    // Upload file if handler exists
                    if (onFileUpload) {
                        try {
                            const finalUrl = await onFileUpload(file);
                            fileExtension.updateNodeWithUrl(remirrorContext.view, attrs.id, finalUrl);
                            onFileUploadSucceeded?.(file.type, file.size, 'button');
                        } catch (uploadError) {
                            console.error(uploadError);
                            onFileUploadFailed?.(
                                file.type,
                                file.size,
                                'button',
                                FileUploadFailureType.UNKNOWN,
                                `${uploadError}`,
                            );
                            fileExtension.removeNode(remirrorContext.view, attrs.id);
                            notification.error({
                                message: 'Upload Failed',
                                description: 'Something went wrong',
                            });
                        }
                    }
                }),
            );
        } catch (error) {
            console.error(error);
            onFileUploadFailed?.(files[0].type, files[0].size, 'button', FileUploadFailureType.UNKNOWN, `${error}`);
            notification.error({
                message: 'Upload Failed',
                description: 'Something went wrong',
            });
        } finally {
            input.value = '';
            hideDropdown();
        }
    };

    return (
        <ContentWrapper>
            <StyledButton size="sm" onClick={handlebuttonClick}>
                Choose File
            </StyledButton>
            <FileInput ref={fileInputRef} type="file" onChange={handleFileChange} />
            <StyledText color="gray" size="sm" lineHeight="normal">
                Max size: 2GB
            </StyledText>
        </ContentWrapper>
    );
};
