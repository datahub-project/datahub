/**
 * Utility functions for file handling in the editor
 */

export const FILE_ATTRS = {
    url: 'data-file-url',
    name: 'data-file-name',
    type: 'data-file-type',
    size: 'data-file-size',
    id: 'data-file-id',
};

export type FileNodeAttributes = {
    url: string;
    name: string;
    type: string;
    size: number;
    id: string;
};

export const SUPPORTED_FILE_TYPES = [
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/webp',
    'application/pdf',
    'text/plain',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
];

/**
 * Generate a unique ID for file nodes
 */
export const generateFileId = (): string => {
    return `file_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
};

/**
 * Format file size in human-readable format
 */
export const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';

    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return `${parseFloat((bytes / k ** i).toFixed(2))} ${sizes[i]}`;
};

/**
 * Check if a file type is supported
 */
export const isFileTypeSupported = (fileType: string, supportedTypes: string[] = SUPPORTED_FILE_TYPES): boolean => {
    return supportedTypes.includes(fileType);
};

/**
 * Get file icon based on file type
 */
export const getFileIconType = (type: string): 'image' | 'pdf' | 'document' | 'other' => {
    if (type.startsWith('image/')) return 'image';
    if (type === 'application/pdf') return 'pdf';
    if (type.includes('document') || type.includes('sheet')) return 'document';
    return 'other';
};

/**
 * Create file node attributes from a File object
 */
export const createFileNodeAttributes = (file: File): FileNodeAttributes => {
    return {
        url: '', // Will be filled after upload
        name: file.name,
        type: file.type,
        size: file.size,
        id: generateFileId(),
    };
};

/**
 * Validate file before processing
 */
export const validateFile = (
    file: File,
    options?: {
        maxSize?: number; // in bytes
        allowedTypes?: string[];
    },
): { isValid: boolean; error?: string } => {
    const { maxSize = 10 * 1024 * 1024, allowedTypes = SUPPORTED_FILE_TYPES } = options || {};

    // Check file size
    if (file.size > maxSize) {
        return {
            isValid: false,
            error: `File size (${(file.size / 1024 / 1024).toFixed(2)}MB) exceeds maximum allowed size (${(maxSize / 1024 / 1024).toFixed(2)}MB)`,
        };
    }

    // Check file type
    if (!isFileTypeSupported(file.type, allowedTypes)) {
        return {
            isValid: false,
            error: `File type "${file.type}" is not allowed. Supported types: ${allowedTypes.join(', ')}`,
        };
    }

    return { isValid: true };
};

/**
 * Handle file download
 */
export const handleFileDownload = (url: string, name: string): void => {
    if (!url) return;

    // Try to open in new tab first
    const newWindow = window.open(url, '_blank');

    // If window.open was blocked (popup blocker), fall back to direct download
    if (!newWindow) {
        const link = document.createElement('a');
        link.href = url;
        link.download = name;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
};
