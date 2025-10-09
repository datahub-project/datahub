import { DownloadSimple, File, FileImage, FilePdf } from '@phosphor-icons/react';
import { NodeViewComponentProps } from '@remirror/react';
import { Button, Card, Image, Spin, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import colors from '@components/theme/foundations/colors';
import { FILE_ATTRS, FileNodeAttributes, formatFileSize, getFileIconType, handleFileDownload } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';

const { Text } = Typography;

const FileContainer = styled.div`
    margin: 8px 0;
    max-width: 100%;

    /* Ensure this container gets the data attributes for markdown conversion */
    &.file-node {
        /* This will help with debugging */
    }
`;

const FileCard = styled(Card)`
    border: 1px solid ${colors.gray[300]};
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);

    &:hover {
        border-color: ${colors.blue[500]};
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }

    .ant-card-body {
        padding: 12px 16px;
    }
`;

const FileInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

const FileIcon = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    border-radius: 6px;
    background-color: ${colors.gray[100]};
`;

const FileDetails = styled.div`
    flex: 1;
    min-width: 0; // Allows text truncation
`;

const FileName = styled(Text)`
    font-weight: 600;
    color: ${colors.gray[1800]};
    display: block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const FileSize = styled(Text)`
    font-size: 12px;
    color: ${colors.gray[1200]};
`;

const ImageContainer = styled.div`
    text-align: center;
    margin: 8px 0;
`;

const StyledImage = styled(Image)`
    max-width: 100%;
    max-height: 400px;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
`;

const LoadingContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 20px;
    background-color: ${colors.gray[50]};
    border-radius: 8px;
    border: 2px dashed ${colors.gray[300]};
`;

interface FileNodeViewProps extends NodeViewComponentProps {
    node: {
        attrs: FileNodeAttributes;
    };
}

const getFileIcon = (type: string) => {
    const iconType = getFileIconType(type);
    
    switch (iconType) {
        case 'image':
            return <FileImage size={24} color={colors.blue[600]} />;
        case 'pdf':
            return <FilePdf size={24} color={colors.red[600]} />;
        case 'document':
            return <File size={24} color={colors.green[600]} />;
        default:
            return <File size={24} color={colors.gray[800]} />;
    }
};

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
                <LoadingContainer>
                    <Spin size="small" />
                    <Text style={{ marginLeft: 8 }}>Uploading {name}...</Text>
                </LoadingContainer>
            </FileContainer>
        );
    }

    // Render images directly
    if (type.startsWith('image/')) {
        return (
            <FileContainer {...containerProps}>
                <ImageContainer>
                    <StyledImage
                        src={url}
                        alt={name}
                        preview={{
                            mask: <div>Click to preview</div>,
                        }}
                        fallback="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAMIAAADDCAYAAADQvc6UAAABRWlDQ1BJQ0MgUHJvZmlsZQAAKJFjYGASSSwoyGFhYGDIzSspCnJ3UoiIjFJgf8LAwSDCIMogwMCcmFxc4BgQ4ANUwgCjUcG3awyMIPqyLsis7PPOq3QdDFcvjV3jOD1boQVTPQrgSkktTgbSf4A4LbmgqISBgTEFyFYuLykAsTuAbJEioKOA7DkgdjqEvQHEToKwj4DVhAQ5A9k3gGyB5IxEoBmML4BsnSQk8XQkNtReEOBxcfXxUQg1Mjc0dyHgXNJBSWpFCYh2zi+oLMpMzyhRcASGUqqCZ16yno6CkYGRAQMDKMwhqj/fAIcloxgHQqxAjIHBEugw5sUIsSQpBobtQPdLciLEVJYzMPBHMDBsayhILEqEO4DxG0txmrERhM29nYGBddr//5/DGRjYNRkY/l7////39v///y4Dmn+LgeHANwDrkl1AuO+pmgAAADhlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAAqACAAQAAAABAAAAwqADAAQAAAABAAAAwwAAAAD9b/HnAAAHlklEQVR4Ae3dP3Ik1RnG4W+FgYxN"
                    />
                </ImageContainer>
                <FileCard size="small">
                    <FileInfo>
                        <FileIcon>{getFileIcon(type)}</FileIcon>
                        <FileDetails>
                            <FileName>{name}</FileName>
                            <FileSize>{formatFileSize(size)}</FileSize>
                        </FileDetails>
                        <Button
                            type="text"
                            icon={<DownloadSimple size={16} />}
                            onClick={() => handleFileDownload(url, name)}
                            title="Download file"
                        />
                    </FileInfo>
                </FileCard>
            </FileContainer>
        );
    }

    // Render other file types as downloadable cards
    return (
        <FileContainer {...containerProps}>
            <FileCard hoverable style={{ cursor: 'pointer' }}>
                <FileInfo>
                    <FileIcon>{getFileIcon(type)}</FileIcon>
                    <FileDetails>
                        <FileName>{name}</FileName>
                        <FileSize>{formatFileSize(size)} • Click to download</FileSize>
                    </FileDetails>
                    <Button
                        type="text"
                        icon={<DownloadSimple size={16} />}
                        onClick={(e) => {
                            e.stopPropagation();
                            handleFileDownload(url, name);
                        }}
                        title="Download file"
                    />
                </FileInfo>
            </FileCard>
        </FileContainer>
    );
};
