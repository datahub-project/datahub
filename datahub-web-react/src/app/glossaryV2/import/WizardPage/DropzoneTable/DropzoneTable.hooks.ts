/**
 * Hooks for DropzoneTable component
 */

import { useState, useCallback } from 'react';
import { FileUploadState } from '../../glossary.types';

export function useFileUpload() {
  const [uploadState, setUploadState] = useState<FileUploadState>({
    file: null,
    isUploading: false,
    isProcessing: false,
    progress: 0,
    error: null
  });

  const selectFile = useCallback((file: File) => {
    setUploadState(prev => ({
      ...prev,
      file,
      error: null,
      progress: 0
    }));
  }, []);

  const removeFile = useCallback(() => {
    setUploadState({
      file: null,
      isUploading: false,
      isProcessing: false,
      progress: 0,
      error: null
    });
  }, []);

  const startProcessing = useCallback(() => {
    setUploadState(prev => ({
      ...prev,
      isProcessing: true,
      progress: 0,
      error: null
    }));
  }, []);

  const updateProgress = useCallback((progress: number) => {
    setUploadState(prev => ({
      ...prev,
      progress: Math.min(100, Math.max(0, progress))
    }));
  }, []);

  const setError = useCallback((error: string) => {
    setUploadState(prev => ({
      ...prev,
      error,
      isProcessing: false,
      isUploading: false
    }));
  }, []);

  const completeProcessing = useCallback(() => {
    setUploadState(prev => ({
      ...prev,
      isProcessing: false,
      progress: 100
    }));
  }, []);

  return {
    uploadState,
    selectFile,
    removeFile,
    startProcessing,
    updateProgress,
    setError,
    completeProcessing
  };
}

export function useFileValidation() {
  const validateFile = useCallback((
    file: File,
    acceptedTypes: string[] = ['.csv'],
    maxSizeMB: number = 10
  ): { isValid: boolean; error?: string } => {
    // Check file type
    const fileExtension = '.' + file.name.split('.').pop()?.toLowerCase();
    if (!acceptedTypes.includes(fileExtension)) {
      return {
        isValid: false,
        error: `File type not supported. Please upload a ${acceptedTypes.join(' or ')} file.`
      };
    }

    // Check file size
    const fileSizeMB = file.size / (1024 * 1024);
    if (fileSizeMB > maxSizeMB) {
      return {
        isValid: false,
        error: `File size exceeds ${maxSizeMB}MB limit. Current size: ${fileSizeMB.toFixed(2)}MB`
      };
    }

    // Check if file is empty
    if (file.size === 0) {
      return {
        isValid: false,
        error: 'File is empty. Please select a valid CSV file.'
      };
    }

    return { isValid: true };
  }, []);

  return { validateFile };
}
