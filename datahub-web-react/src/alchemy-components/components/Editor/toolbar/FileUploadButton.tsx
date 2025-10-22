import { Button, Dropdown, Text, Tooltip, colors, notification } from '@components';
import { useRemirrorContext } from '@remirror/react';
import { FileArrowUp } from 'phosphor-react';
import React, { useRef, useState } from 'react';
import styled from 'styled-components';

import {
    FileDragDropExtension,
    SUPPORTED_FILE_TYPES,
    createFileNodeAttributes,
    validateFile,
} from '@components/components/Editor/extensions/fileDragDrop';
import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';

const DropdownContainer = styled.div`
    box-shadow: 0 4px 12px 0 rgba(9, 1, 61, 0.12);
    display: flex;
    flex-direction: column;
    padding: 8px;
    gap: 8px;
    border-radius: 12px;
    width: 192px;
    background: ${colors.white};
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

export const FileUploadButton = () => {
    const { commands } = useRemirrorContext();

    const fileInputRef = useRef<HTMLInputElement>(null);
    const remirrorContext = useRemirrorContext();
    const fileExtension = remirrorContext.getExtension(FileDragDropExtension);

    const [showDropdown, setShowDropdown] = useState(false);

    const handlebuttonClick = () => {
        fileInputRef.current?.click();
    };

    const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const input = event.target as HTMLInputElement;
        const files = input.files ? Array.from(input.files) : [];
        if (files.length === 0) return;

        const supportedTypes = SUPPORTED_FILE_TYPES;
        const { onFileUpload } = fileExtension.options;

        try {
            // Process files concurrently
            await Promise.all(
                files.map(async (file) => {
                    const validation = validateFile(file, { allowedTypes: supportedTypes });
                    if (!validation.isValid) {
                        console.error(validation.error);
                        notification.error({
                            message: 'Upload Failed',
                            description: validation.displayError || validation.error,
                        });
                    }

                    // Create placeholder node
                    const attrs = createFileNodeAttributes(file);
                    commands.insertFileNode({ ...attrs, url: '' });

                    // Upload file if handler exists
                    if (onFileUpload) {
                        try {
                            const finalUrl = await onFileUpload(file);
                            fileExtension.updateNodeWithUrl(remirrorContext.view, attrs.id, finalUrl);
                        } catch (uploadError) {
                            console.error(uploadError);
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
            notification.error({
                message: 'Upload Failed',
                description: 'Something went wrong',
            });
        } finally {
            input.value = '';
            setShowDropdown(false);
        }
    };

    const dropdownContent = () => (
        <DropdownContainer>
            <StyledButton size="sm" onClick={handlebuttonClick}>
                Choose File
            </StyledButton>
            <FileInput ref={fileInputRef} type="file" onChange={handleFileChange} />
            <StyledText color="gray" size="sm" lineHeight="normal">
                Max size: 2GB
            </StyledText>
        </DropdownContainer>
    );

    return (
        <Dropdown open={showDropdown} onOpenChange={(open) => setShowDropdown(open)} dropdownRender={dropdownContent}>
            <Tooltip title="Upload File">
                <CommandButton
                    icon={<FileArrowUp size={20} color={colors.gray[1800]} />}
                    onClick={() => setShowDropdown(true)}
                    commandName="uploadFile"
                />
            </Tooltip>
        </Dropdown>
    );
};
