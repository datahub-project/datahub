import { DownloadSimple, File, FileImage, FilePdf } from '@phosphor-icons/react';
import { NodeViewComponentProps } from '@remirror/react';
import { Button, Card, Image, Spin, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { FILE_ATTRS } from '@components/components/Editor/extensions/fileDragDrop/FileDragDropExtension';

import { colors } from '@src/alchemy-components/theme';

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
        attrs: {
            url: string;
            name: string;
            type: string;
            size: number;
        };
    };
}

const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
};

const getFileIcon = (type: string) => {
    if (type.startsWith('image/')) {
        return <FileImage size={24} color={colors.blue[600]} />;
    }
    if (type === 'application/pdf') {
        return <FilePdf size={24} color={colors.red[600]} />;
    }
    return <File size={24} color={colors.gray[800]} />;
};

const handleDownload = (url: string, name: string) => {
    if (!url) return;
    
    const link = document.createElement('a');
    link.href = url;
    link.download = name;
    link.target = '_blank';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
};

export const FileNodeView: React.FC<FileNodeViewProps> = ({ node }) => {
    const { url, name, type, size } = node.attrs;
    
    console.log('🔥 FileNodeView rendering with attrs:', { url, name, type, size });
    
    // Create props with data attributes for markdown conversion
    // These must match exactly what toDOM creates in the extension
    const containerProps = {
        className: 'file-node',
        [FILE_ATTRS.url]: url,
        [FILE_ATTRS.name]: name,
        [FILE_ATTRS.type]: type,
        [FILE_ATTRS.size]: size.toString(),
        // Also add the standard data- attributes that HTML expects
        'data-file-url': url,
        'data-file-name': name,
        'data-file-type': type,
        'data-file-size': size.toString(),
    };
    
    console.log('🔥 Container props:', containerProps);
    
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
                            onClick={() => handleDownload(url, name)}
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
            <FileCard
                hoverable
                onClick={() => handleDownload(url, name)}
                style={{ cursor: 'pointer' }}
            >
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
                            handleDownload(url, name);
                        }}
                        title="Download file"
                    />
                </FileInfo>
            </FileCard>
        </FileContainer>
    );
};