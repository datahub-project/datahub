/**
 * DropzoneTable component for file upload with drag-and-drop
 */
import { Button, Icon, Text } from '@components';
import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { Upload } from '@phosphor-icons/react/dist/csr/Upload';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

const DropzoneContainer = styled.div<{ isDragActive: boolean; hasFile: boolean }>`
    border: 2px dashed
        ${(props) => {
            if (props.isDragActive) return props.theme.colors.borderInformation;
            if (props.hasFile) return props.theme.colors.borderSuccess;
            return props.theme.colors.border;
        }};
    border-radius: 8px;
    padding: 48px 24px;
    text-align: center;
    background-color: ${(props) => {
        if (props.isDragActive || props.hasFile) return props.theme.colors.bgSurfaceSuccess;
        return props.theme.colors.bgSurface;
    }};
    transition: all 0.3s ease;
    cursor: pointer;
    position: relative;

    &:hover {
        border-color: ${(props) => props.theme.colors.borderInformation};
        background-color: ${(props) => props.theme.colors.bgSurfaceSuccess};
    }
`;

const DropzoneContent = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
`;

const UploadIcon = styled.div`
    font-size: 48px;
    color: ${(props) => props.theme.colors.textSecondary};
    display: flex;
    align-items: center;
    justify-content: center;
`;

const StatusIcon = styled.div<{ status: 'success' | 'error' | 'processing' }>`
    width: 32px;
    height: 32px;
    display: flex;
    align-items: center;
    justify-content: center;

    svg {
        width: 24px;
        height: 24px;
        color: ${(props) => {
            if (props.status === 'success') return props.theme.colors.iconSuccess;
            if (props.status === 'error') return props.theme.colors.iconError;
            return props.theme.colors.iconInformation;
        }};
    }
`;

const FileInfo = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
`;

const FileName = styled(Text)`
    font-weight: 500;
    font-size: 16px;
`;

const FileSize = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 14px;
`;

const ProgressContainer = styled.div`
    width: 100%;
    max-width: 400px;
`;

const CustomProgressBar = styled.div<{ progress: number }>`
    width: 100%;
    height: 8px;
    background-color: ${(props) => props.theme.colors.border};
    border-radius: 4px;
    overflow: hidden;

    &::after {
        content: '';
        display: block;
        width: ${(props) => props.progress}%;
        height: 100%;
        background-color: ${(props) => props.theme.colors.iconInformation};
        border-radius: 4px;
        transition: width 0.3s ease;
    }
`;

const ActionButtons = styled.div`
    display: flex;
    gap: 12px;
    margin-top: 16px;
`;

const HiddenInput = styled.input`
    display: none;
`;

interface DropzoneTableProps {
    onFileSelect: (file: File) => void;
    onFileRemove: () => void;
    file: File | null;
    isProcessing: boolean;
    progress: number;
    error: string | null;
    acceptedFileTypes?: string[];
    maxFileSize?: number; // in MB
    onRetry?: () => void;
    canRetry?: boolean;
}

export default function DropzoneTable({
    onFileSelect,
    onFileRemove,
    file,
    isProcessing,
    progress,
    error,
    acceptedFileTypes = ['.csv'],
    maxFileSize = 10,
    onRetry,
    canRetry = false,
}: DropzoneTableProps) {
    const { t } = useTranslation('governance.glossary');
    const theme = useTheme();
    const validateAndSelectFile = useCallback(
        (selectedFile: File) => {
            // Validate file type
            const fileExtension = `.${selectedFile.name.split('.').pop()?.toLowerCase()}`;
            if (!acceptedFileTypes.includes(fileExtension)) {
                // This would be handled by the parent component
                return;
            }

            // Validate file size
            const fileSizeMB = selectedFile.size / (1024 * 1024);
            if (fileSizeMB > maxFileSize) {
                // This would be handled by the parent component
                return;
            }

            onFileSelect(selectedFile);
        },
        [acceptedFileTypes, maxFileSize, onFileSelect],
    );

    const handleDragOver = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
    }, []);

    const handleDragEnter = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
    }, []);

    const handleDragLeave = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
    }, []);

    const handleDrop = useCallback(
        (e: React.DragEvent) => {
            e.preventDefault();
            e.stopPropagation();

            const files = Array.from(e.dataTransfer.files);
            if (files.length > 0) {
                const droppedFile = files[0];
                validateAndSelectFile(droppedFile);
            }
        },
        [validateAndSelectFile],
    );

    const handleFileInputChange = useCallback(
        (e: React.ChangeEvent<HTMLInputElement>) => {
            const { files: inputFiles } = e.target;
            if (inputFiles && inputFiles.length > 0) {
                const selectedFile = inputFiles[0];
                validateAndSelectFile(selectedFile);
            }
        },
        [validateAndSelectFile],
    );

    const handleClick = useCallback(() => {
        if (!file && !isProcessing) {
            const input = document.getElementById('file-input') as HTMLInputElement;
            input?.click();
        }
    }, [file, isProcessing]);

    const formatFileSize = (bytes: number): string => {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${parseFloat((bytes / k ** i).toFixed(2))} ${sizes[i]}`;
    };

    const getStatusIcon = () => {
        if (error) {
            return (
                <StatusIcon status="error">
                    <Icon icon={Warning} size="md" color="red" />
                </StatusIcon>
            );
        }
        if (file && !isProcessing) {
            return (
                <StatusIcon status="success">
                    <Icon icon={CheckCircle} size="md" color="green" />
                </StatusIcon>
            );
        }
        if (isProcessing) {
            return (
                <StatusIcon status="processing">
                    <Icon icon={Upload} size="md" color="blue" />
                </StatusIcon>
            );
        }
        return (
            <UploadIcon>
                <Icon icon={Upload} size="lg" color="gray" />
            </UploadIcon>
        );
    };

    const renderContent = () => {
        if (file) {
            return (
                <DropzoneContent>
                    {getStatusIcon()}
                    <FileInfo>
                        <FileName>{file.name}</FileName>
                        <FileSize>{formatFileSize(file.size)}</FileSize>
                    </FileInfo>

                    {isProcessing && (
                        <ProgressContainer>
                            <CustomProgressBar progress={Math.round(progress)} />
                        </ProgressContainer>
                    )}

                    {error && (
                        <div
                            style={{
                                padding: '12px',
                                backgroundColor: theme.colors.bgSurfaceError,
                                border: `1px solid ${theme.colors.borderError}`,
                                borderRadius: '6px',
                                color: theme.colors.textError,
                                fontSize: '14px',
                                maxWidth: 400,
                            }}
                        >
                            <strong>{t('import.dropzone.errorLabel')}</strong> {error}
                        </div>
                    )}

                    {!isProcessing && (
                        <ActionButtons>
                            {canRetry && onRetry && (
                                <Button variant="filled" color="primary" onClick={onRetry}>
                                    {t('import.dropzone.retry')}
                                </Button>
                            )}
                            <Button onClick={handleClick} disabled={isProcessing}>
                                {t('import.dropzone.chooseDifferentFile')}
                            </Button>
                            <Button variant="filled" color="red" onClick={onFileRemove}>
                                {t('import.dropzone.removeFile')}
                            </Button>
                        </ActionButtons>
                    )}
                </DropzoneContent>
            );
        }

        return (
            <DropzoneContent>
                {getStatusIcon()}
                <div>
                    <Text weight="bold" style={{ fontSize: 16 }}>
                        {isProcessing ? t('import.dropzone.processing') : t('import.dropzone.dropPrompt')}
                    </Text>
                    <br />
                    <Text color="gray">{t('import.dropzone.clickToBrowse')}</Text>
                </div>

                {isProcessing && (
                    <ProgressContainer>
                        <CustomProgressBar progress={Math.round(progress)} />
                    </ProgressContainer>
                )}

                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', alignItems: 'center' }}>
                    <Text style={{ fontSize: 12, color: theme.colors.textTertiary }}>
                        {t('import.dropzone.supportedFormats', { formats: acceptedFileTypes.join(', ') })}
                    </Text>
                    <Text style={{ fontSize: 12, color: theme.colors.textTertiary }}>
                        {t('import.dropzone.maxFileSize', { size: maxFileSize })}
                    </Text>
                </div>
            </DropzoneContent>
        );
    };

    return (
        <>
            <DropzoneContainer
                data-testid="dropzone-table"
                isDragActive={false} // This would be managed by parent component
                hasFile={!!file}
                onDragOver={handleDragOver}
                onDragEnter={handleDragEnter}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
                onClick={handleClick}
            >
                {renderContent()}
            </DropzoneContainer>

            <HiddenInput
                id="file-input"
                type="file"
                // eslint-disable-next-line i18next/no-literal-string
                accept={acceptedFileTypes.join(',')}
                onChange={handleFileInputChange}
                disabled={isProcessing}
            />
        </>
    );
}
