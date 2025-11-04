import { NodeViewComponentProps } from '@remirror/react';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    FILE_ATTRS,
    FileNodeAttributes,
    getExtensionFromFileName,
    getFileIconFromExtension,
    handleFileDownload,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { Icon } from '@components/components/Icon';
import { colors } from '@components/theme';

import Loading from '@app/shared/Loading';

const FileContainer = styled.span`
    width: fit-content;
    display: inline-block;
    padding: 4px;
    cursor: pointer;
    color: ${({ theme }) => theme.styles['primary-color']};

    :hover {
        border-radius: 8px;
        background-color: ${colors.gray[1500]};
    }

    .ProseMirror-selectednode > & {
        border-radius: 8px;
        background-color: ${colors.gray[1500]};
    }
`;

const FileDetails = styled.span`
    width: fit-content;
    display: flex;
    gap: 4px;
    align-items: center;
    font-weight: 600;
    max-width: 350px;
`;

const FileName = styled(Typography.Text)`
    color: ${({ theme }) => theme.styles['primary-color']};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

interface FileNodeViewProps extends NodeViewComponentProps {
    node: {
        attrs: FileNodeAttributes;
    };
    onFileDownloadView?: (fileType: string, fileSize: number) => void;
}

export const FileNodeView: React.FC<FileNodeViewProps> = ({ node, onFileDownloadView }) => {
    const { url, name, type, size, id } = node.attrs;

    // Create props with data attributes for markdown conversion
    // These must match exactly what toDOM creates in the extension
    const containerProps = {
        className: 'file-node',
        [FILE_ATTRS.url]: url,
        [FILE_ATTRS.name]: name,
        [FILE_ATTRS.type]: type,
        [FILE_ATTRS.size]: size.toString(),
        [FILE_ATTRS.id]: id,
    };

    // Show loading state if no URL yet (file is being uploaded)
    if (!url) {
        return (
            <FileContainer {...containerProps}>
                <FileDetails>
                    <Loading height={18} width={20} marginTop={0} />
                    <FileName>Uploading {name}...</FileName>
                </FileDetails>
            </FileContainer>
        );
    }

    const extension = getExtensionFromFileName(name);
    const icon = getFileIconFromExtension(extension || '');

    return (
        <FileContainer {...containerProps}>
            <FileDetails
                onClick={(e) => {
                    e.stopPropagation();
                    // Track file download/view event
                    onFileDownloadView?.(type, size);
                    handleFileDownload(url, name);
                }}
            >
                <Icon icon={icon} size="lg" source="phosphor" />
                <FileName ellipsis={{ tooltip: name }}>{name}</FileName>
            </FileDetails>
        </FileContainer>
    );
};
