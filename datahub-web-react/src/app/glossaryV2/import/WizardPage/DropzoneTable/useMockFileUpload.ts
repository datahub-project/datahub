import { useState, useCallback } from 'react';
import { mockFileUploadStates } from '../../shared/mocks/mockData';

export interface FileUploadState {
  isDragOver: boolean;
  isUploading: boolean;
  isProcessing: boolean;
  isComplete: boolean;
  progress: number;
  error: string | null;
  file: File | null;
}

export interface UseMockFileUploadReturn {
  uploadState: FileUploadState;
  selectFile: (file: File) => void;
  removeFile: () => void;
  startProcessing: () => void;
  completeProcessing: () => void;
  setError: (error: string) => void;
  updateProgress: (progress: number) => void;
  reset: () => void;
}

export function useMockFileUpload(): UseMockFileUploadReturn {
  const [uploadState, setUploadState] = useState<FileUploadState>(mockFileUploadStates.idle);

  const selectFile = useCallback((file: File) => {
    setUploadState(prev => ({
      ...prev,
      file,
      isUploading: true,
      progress: 0,
      error: null
    }));
  }, []);

  const removeFile = useCallback(() => {
    setUploadState(mockFileUploadStates.idle);
  }, []);

  const startProcessing = useCallback(() => {
    setUploadState(prev => ({
      ...prev,
      isUploading: false,
      isProcessing: true,
      progress: 50
    }));
  }, []);

  const completeProcessing = useCallback(() => {
    setUploadState(prev => ({
      ...prev,
      isProcessing: false,
      isComplete: true,
      progress: 100
    }));
  }, []);

  const setError = useCallback((error: string) => {
    setUploadState(prev => ({
      ...prev,
      isUploading: false,
      isProcessing: false,
      isComplete: false,
      error,
      progress: 0
    }));
  }, []);

  const updateProgress = useCallback((progress: number) => {
    setUploadState(prev => ({
      ...prev,
      progress
    }));
  }, []);

  const reset = useCallback(() => {
    setUploadState(mockFileUploadStates.idle);
  }, []);

  return {
    uploadState,
    selectFile,
    removeFile,
    startProcessing,
    completeProcessing,
    setError,
    updateProgress,
    reset
  };
}

export interface UseMockFileValidationReturn {
  validateFile: (file: File) => { isValid: boolean; error?: string };
}

export function useMockFileValidation(): UseMockFileValidationReturn {
  const validateFile = useCallback((file: File) => {
    // Mock validation logic
    if (!file.name.endsWith('.csv')) {
      return {
        isValid: false,
        error: 'File must be a CSV file'
      };
    }

    if (file.size > 10 * 1024 * 1024) { // 10MB
      return {
        isValid: false,
        error: 'File size must be less than 10MB'
      };
    }

    return {
      isValid: true
    };
  }, []);

  return {
    validateFile
  };
}
