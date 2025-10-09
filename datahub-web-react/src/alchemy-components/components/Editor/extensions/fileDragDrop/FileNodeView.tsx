import { NodeViewComponentProps } from '@remirror/react';
import React from 'react';
import styled from 'styled-components';

import {
    FILE_ATTRS,
    FileNodeAttributes,
    handleFileDownload,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { Icon } from '@components/components/Icon';

import Loading from '@app/shared/Loading';

const FileContainer = styled.div`
    max-width: 100%;

    /* Ensure this container gets the data attributes for markdown conversion */
    &.file-node {
        /* This will help with debugging */
    }
`;

const FileDetails = styled.div`
    min-width: 0; // Allows text truncation
    cursor: pointer;
    display: flex;
    gap: 4px;
    align-items: center;
    color: ${({ theme }) => theme.styles['primary-color']};
    font-weight: 600;
`;

const FileName = styled.span`
    color: ${({ theme }) => theme.styles['primary-color']};
    display: block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

interface FileNodeViewProps extends NodeViewComponentProps {
    node: {
        attrs: FileNodeAttributes;
    };
}

export const FileNodeView: React.FC<FileNodeViewProps> = ({ node }) => {
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

    return (
        <FileContainer {...containerProps}>
            <FileDetails
                onClick={(e) => {
                    e.stopPropagation();
                    handleFileDownload(url, name);
                }}
            >
                <Icon icon="FileArrowDown" size="lg" source="phosphor" />
                <FileName>{name}</FileName>
            </FileDetails>
        </FileContainer>
    );
};
