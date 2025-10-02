/**
 * DropzoneTable component for file upload with drag-and-drop
 */

import React, { useCallback } from 'react';
import styled from 'styled-components';
import { Button } from '@components';
import { Progress, Alert, Typography, Space } from 'antd';
import { UploadOutlined, FileTextOutlined, ExclamationCircleOutlined, CheckCircleOutlined } from '@ant-design/icons';

const { Text } = Typography;

const DropzoneContainer = styled.div<{ isDragActive: boolean; hasFile: boolean }>`
  border: 2px dashed ${props => 
    props.isDragActive ? '#1890ff' : 
    props.hasFile ? '#52c41a' : '#d9d9d9'
  };
  border-radius: 8px;
  padding: 48px 24px;
  text-align: center;
  background-color: ${props => 
    props.isDragActive ? '#f6ffed' : 
    props.hasFile ? '#f6ffed' : '#fafafa'
  };
  transition: all 0.3s ease;
  cursor: pointer;
  position: relative;
  
  &:hover {
    border-color: #1890ff;
    background-color: #f6ffed;
  }
`;

const DropzoneContent = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 16px;
`;

const UploadIcon = styled(UploadOutlined)`
  font-size: 48px;
  color: #8c8c8c;
`;

const FileIcon = styled(FileTextOutlined)`
  font-size: 32px;
  color: #52c41a;
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
    color: ${props => 
      props.status === 'success' ? '#52c41a' :
      props.status === 'error' ? '#ff4d4f' :
      '#1890ff'
    };
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
  color: #8c8c8c;
  font-size: 14px;
`;

const ProgressContainer = styled.div`
  width: 100%;
  max-width: 400px;
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
}

export default function DropzoneTable({
  onFileSelect,
  onFileRemove,
  file,
  isProcessing,
  progress,
  error,
  acceptedFileTypes = ['.csv'],
  maxFileSize = 10
}: DropzoneTableProps) {
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

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();

    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      const droppedFile = files[0];
      validateAndSelectFile(droppedFile);
    }
  }, []);

  const handleFileInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      const selectedFile = files[0];
      validateAndSelectFile(selectedFile);
    }
  }, []);

  const validateAndSelectFile = useCallback((file: File) => {
    // Validate file type
    const fileExtension = '.' + file.name.split('.').pop()?.toLowerCase();
    if (!acceptedFileTypes.includes(fileExtension)) {
      // This would be handled by the parent component
      return;
    }

    // Validate file size
    const fileSizeMB = file.size / (1024 * 1024);
    if (fileSizeMB > maxFileSize) {
      // This would be handled by the parent component
      return;
    }

    onFileSelect(file);
  }, [acceptedFileTypes, maxFileSize, onFileSelect]);

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
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const getStatusIcon = () => {
    if (error) {
      return (
        <StatusIcon status="error">
          <ExclamationCircleOutlined />
        </StatusIcon>
      );
    }
    if (file && !isProcessing) {
      return (
        <StatusIcon status="success">
          <CheckCircleOutlined />
        </StatusIcon>
      );
    }
    if (isProcessing) {
      return (
        <StatusIcon status="processing">
          <UploadOutlined />
        </StatusIcon>
      );
    }
    return <UploadIcon />;
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
              <Progress 
                percent={Math.round(progress)} 
                status={error ? 'exception' : 'active'}
                strokeColor="#52c41a"
              />
            </ProgressContainer>
          )}
          
          {error && (
            <Alert
              message="Upload Error"
              description={error}
              type="error"
              showIcon
              style={{ maxWidth: 400 }}
            />
          )}
          
          {!isProcessing && (
            <ActionButtons>
              <Button onClick={handleClick} disabled={isProcessing}>
                Choose Different File
              </Button>
              <Button variant="filled" color="red" onClick={onFileRemove}>
                Remove File
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
          <Text strong style={{ fontSize: 16 }}>
            {isProcessing ? 'Processing file...' : 'Drop your CSV file here'}
          </Text>
          <br />
          <Text type="secondary">
            or click to browse files
          </Text>
        </div>
        
        {isProcessing && (
          <ProgressContainer>
            <Progress 
              percent={Math.round(progress)} 
              status="active"
              strokeColor="#1890ff"
            />
          </ProgressContainer>
        )}
        
        <Space direction="vertical" size="small">
          <Text type="secondary" style={{ fontSize: 12 }}>
            Supported formats: {acceptedFileTypes.join(', ')}
          </Text>
          <Text type="secondary" style={{ fontSize: 12 }}>
            Maximum file size: {maxFileSize}MB
          </Text>
        </Space>
      </DropzoneContent>
    );
  };

  return (
    <>
      <DropzoneContainer
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
        accept={acceptedFileTypes.join(',')}
        onChange={handleFileInputChange}
        disabled={isProcessing}
      />
    </>
  );
}
