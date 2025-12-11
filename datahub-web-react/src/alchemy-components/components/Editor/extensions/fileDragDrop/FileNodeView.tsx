/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { NodeViewComponentProps } from '@remirror/react';
import { Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import {
    FILE_ATTRS,
    FILE_TYPES_TO_PREVIEW,
    FileNodeAttributes,
    TEXT_FILE_TYPES_TO_PREVIEW,
    getExtensionFromFileName,
    getFileIconFromExtension,
    getFileTypeFromFilename,
    handleFileDownload,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { Icon } from '@components/components/Icon';
import { colors } from '@components/theme';

import Loading from '@app/shared/Loading';

const StyledIcon = styled(Icon)`
    flex-shrink: 0;
`;

const FileContainer = styled.div<{ $isInline?: boolean }>`
    display: inline-block;

    ${(props) =>
        props.$isInline
            ? `
        width: fit-content;

        .ProseMirror-selectednode & {
            border-radius: 8px;
            background-color: ${colors.gray[1500]};
        }
    `
            : `
        max-width: 100%;
        width: 100%;

    `}

    cursor: pointer;
    color: ${({ theme }) => theme.styles['primary-color']};
`;

const FileDetails = styled.span`
    max-width: 350px;
    display: flex;
    gap: 4px;
    align-items: center;
    font-weight: 600;
    width: max-content;
    padding: 4px;
`;

const FileName = styled(Typography.Text)`
    color: ${({ theme }) => theme.styles['primary-color']};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const StyledSyntaxHighlighter = styled(SyntaxHighlighter)`
    background-color: ${colors.gray[1500]} !important;
    border: none !important;
`;

const PdfWrapper = styled.div`
    resize: both;
    overflow: hidden;
    width: 100%;
    max-width: 100%;
    height: 400px;
    border-radius: 8px;
`;

const PdfViewer = styled.iframe<{ $isResizing?: boolean }>`
    width: 100%;
    height: 100%;
    border: none;
    pointer-events: ${({ $isResizing }) => ($isResizing ? 'none' : 'auto')};
    border-radius: 8px;
    margin-top: 8px;
`;

const VideoContainer = styled.div`
    border-radius: 8px;
    overflow: hidden;
    resize: horizontal;
    min-width: 150px;
    max-width: 100%;
    width: 50%;
    background-color: ${colors.black};
    margin-top: 8px;
`;

const VideoPlayer = styled.video`
    width: 100%;
    height: auto;
    border-radius: 8px;
`;

const FileNameButtonWrapper = styled.div`
    display: flex;
    align-items: center;
    width: fit-content;

    :hover {
        border-radius: 8px;
        background-color: ${colors.gray[1500]};
    }
`;

interface FileNodeViewProps extends NodeViewComponentProps {
    node: {
        attrs: FileNodeAttributes;
    };
    onFileDownloadView?: (fileType: string, fileSize: number) => void;
}

export const FileNodeView: React.FC<FileNodeViewProps> = ({ node, onFileDownloadView }) => {
    const [isPreviewVisible, setIsPreviewVisible] = useState(true);
    const { url, name, type, size, id } = node.attrs;
    const extension = getExtensionFromFileName(name);
    const fileType = type || getFileTypeFromFilename(name);
    const icon = getFileIconFromExtension(extension || '');
    const shouldWrap = extension === 'txt';
    const isPdf = fileType === 'application/pdf';
    const isVideo = fileType.startsWith('video/');

    // Create props with data attributes for markdown conversion
    // These must match exactly what toDOM creates in the extension
    const containerProps = {
        className: 'file-node',
        [FILE_ATTRS.url]: url,
        [FILE_ATTRS.name]: name,
        [FILE_ATTRS.type]: fileType,
        [FILE_ATTRS.size]: size.toString(),
        [FILE_ATTRS.id]: id,
    };

    const [fileContent, setFileContent] = useState<string | null>(null);
    const [hasError, setHasError] = useState(false);
    const [pdfError, setPdfError] = useState(false);
    const [videoError, setVideoError] = useState(false);
    const [isResizingPdf, setIsResizingPdf] = useState(false);
    const [hasLoaded, setHasLoaded] = useState(false); // Track initial loading

    // Text-based files for which preview should be shown
    const shouldShowPreview = FILE_TYPES_TO_PREVIEW.some((t) => fileType?.startsWith(t));
    const isTextFile = TEXT_FILE_TYPES_TO_PREVIEW.some((t) => fileType?.startsWith(t));

    useEffect(() => {
        if (!url) return;

        setHasLoaded(false);

        if (shouldShowPreview) {
            fetch(url)
                .then((res) => {
                    if (!res.ok) {
                        setHasError(true);
                        return null;
                    }
                    if (isTextFile) {
                        return res.text();
                    }
                    return null;
                })
                .then((text) => {
                    if (text) setFileContent(text);
                })
                .catch(() => {
                    setHasError(true);
                    setFileContent(null);
                })
                .finally(() => {
                    setHasLoaded(true);
                });
        }
    }, [url, hasError, shouldShowPreview, isTextFile]);

    // Show loading state if no URL yet (file is being uploaded)
    if (!url) {
        return (
            <FileContainer {...containerProps} $isInline>
                <FileDetails>
                    <Loading height={18} width={20} marginTop={0} />
                    <FileName>Uploading {name}...</FileName>
                </FileDetails>
            </FileContainer>
        );
    }

    const fileNode = (
        <FileDetails
            onClick={(e) => {
                e.stopPropagation();
                // Track file download/view event
                onFileDownloadView?.(fileType, size);
                handleFileDownload(url, name);
            }}
        >
            <StyledIcon icon={icon} size="lg" source="phosphor" />
            <FileName ellipsis={{ tooltip: name }}>{name}</FileName>
        </FileDetails>
    );

    const fileNodeWithButton = (
        <FileNameButtonWrapper>
            {fileNode}
            <Button
                icon={{ source: 'phosphor', icon: isPreviewVisible ? 'CaretDown' : 'CaretRight' }}
                variant="text"
                onClick={() => setIsPreviewVisible(!isPreviewVisible)}
            />
        </FileNameButtonWrapper>
    );

    // Preview pdf files
    if (isPdf && !hasError && !pdfError && hasLoaded) {
        return (
            <FileContainer {...containerProps}>
                {fileNodeWithButton}
                {isPreviewVisible && (
                    <PdfWrapper
                        onMouseDown={() => setIsResizingPdf(true)}
                        onMouseUp={() => setIsResizingPdf(false)}
                        onMouseLeave={() => setIsResizingPdf(false)}
                    >
                        <PdfViewer
                            src={url}
                            title={name}
                            onError={() => setPdfError(true)}
                            $isResizing={isResizingPdf}
                        />
                    </PdfWrapper>
                )}
            </FileContainer>
        );
    }

    // Preview video files
    if (isVideo && !hasError && !videoError && hasLoaded) {
        return (
            <FileContainer {...containerProps}>
                {fileNodeWithButton}
                {isPreviewVisible && (
                    <VideoContainer>
                        <VideoPlayer controls preload="metadata" onError={() => setVideoError(true)}>
                            <source src={url} type={fileType} />
                        </VideoPlayer>
                    </VideoContainer>
                )}
            </FileContainer>
        );
    }

    // Preview text files
    if (fileContent !== null) {
        return (
            <FileContainer {...containerProps}>
                {fileNodeWithButton}
                {isPreviewVisible && (
                    <StyledSyntaxHighlighter
                        language={extension || 'text'}
                        customStyle={{
                            maxHeight: 250,
                            borderRadius: 8,
                        }}
                        wrapLongLines={shouldWrap}
                    >
                        {fileContent}
                    </StyledSyntaxHighlighter>
                )}
            </FileContainer>
        );
    }

    // Other files
    return (
        <FileContainer {...containerProps} $isInline>
            {fileNode}
        </FileContainer>
    );
};
