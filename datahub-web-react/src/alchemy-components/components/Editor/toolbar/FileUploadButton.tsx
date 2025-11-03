import { Dropdown, Tooltip, colors } from '@components';
import { useRemirrorContext } from '@remirror/react';
import { FileArrowUp } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { FileDragDropExtension } from '@components/components/Editor/extensions/fileDragDrop';
import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { FileUploadContent } from '@components/components/Editor/toolbar/FileUploadContent';

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

export const FileUploadButton = () => {
    const remirrorContext = useRemirrorContext();
    const fileExtension = remirrorContext.getExtension(FileDragDropExtension);

    const [showDropdown, setShowDropdown] = useState(false);

    // Hide the button when uploading of files is disabled
    if (!fileExtension.options.onFileUpload) return null;

    return (
        <Dropdown
            open={showDropdown}
            onOpenChange={(open) => setShowDropdown(open)}
            dropdownRender={() => (
                <DropdownContainer>
                    <FileUploadContent hideDropdown={() => setShowDropdown(false)} />
                </DropdownContainer>
            )}
        >
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
