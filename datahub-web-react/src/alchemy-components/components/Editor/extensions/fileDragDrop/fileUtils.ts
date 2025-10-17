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
    'image/bmp',
    'image/gif',
    'image/webp',
    'application/pdf',
    'text/plain',
    'text/csv',
    'text/markdown',
    'video/quicktime',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.ms-excel',
    'application/xml',
    'application/vnd.ms-powerpoint',
    'application/msword',
    'application/rtf',
    'application/gzip',
    'application/zip',
    'video/mp4',
    'audio/mpeg',
    'video/x-ms-wmv',
    'image/tiff',
];

const EXTENSION_TO_FILE_TYPE = {
    pdf: 'application/pdf',
    doc: 'application/msword',
    txt: 'text/plain',
    docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    pptx: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    xls: 'application/vnd.ms-excel',
    ppt: 'application/vnd.ms-powerpoint',
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    png: 'image/png',
    gif: 'image/gif',
    webp: 'image/webp',
    mp4: 'video/mp4',
    mp3: 'audio/mpeg',
    zip: 'application/zip',
    rar: 'application/x-rar-compressed',
    xml: 'application/xml',
    bmp: 'image/bmp',
    rtf: 'application/rtf',
    gz: 'application/gzip',
    wmv: 'video/x-ms-wmv',
    tiff: 'image/tiff',
    md: 'text/markdown',
    csv: 'text/csv',
};

/**
 * Generate a unique ID for file nodes
 */
export const generateFileId = (): string => {
    return `file_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
};

/**
 * Check if a file type is supported
 */
export const isFileTypeSupported = (fileType: string, supportedTypes: string[] = SUPPORTED_FILE_TYPES): boolean => {
    return supportedTypes.includes(fileType);
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

const MAX_FILE_SIZE_IN_BYTES = 2 * 1000 * 1000 * 1000; // 2GB

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
    const { maxSize = MAX_FILE_SIZE_IN_BYTES, allowedTypes = SUPPORTED_FILE_TYPES } = options || {};

    // Check file size
    if (file.size > maxSize) {
        return {
            isValid: false,
            error: `File size (${(file.size / 1000 / 1000).toFixed(2)}MB) exceeds maximum allowed size (${(maxSize / 1000 / 1000).toFixed(2)}MB)`,
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

/**
 * Determines if a URL points to a file in our storage system
 * @param url - the URL to check
 * @returns true if the URL is a file URL
 */
export const isFileUrl = (url: string): boolean => {
    // Check for common file storage patterns
    return (
        url.includes('/api/files/') || // Our internal file API
        url.includes('s3.amazonaws.com') || // S3 URLs
        url.includes('amazonaws.com') || // Other AWS storage
        url.includes('storage.googleapis.com') || // Google Cloud Storage
        url.includes('blob.core.windows.net') || // Azure Blob Storage
        url.match(/\.(pdf|doc|docx|xls|xlsx|ppt|pptx|txt|jpg|jpeg|png|gif|webp|mp4|mp3|zip|rar)$/i) !== null // File extensions
    );
};

/**
 * Extract file type from URL
 * @param url - the URL to extract type from
 * @returns MIME type if detectable, empty string otherwise
 */
export const getFileTypeFromUrl = (url: string): string => {
    const extension = url.split('.').pop()?.toLowerCase();
    if (!extension) return '';

    return EXTENSION_TO_FILE_TYPE[extension] || '';
};

/**
 * Extract file type from filename
 * @param filename - the filename to extract type from
 * @returns MIME type if detectable, empty string otherwise
 */
export const getFileTypeFromFilename = (filename: string): string => {
    return getFileTypeFromUrl(filename);
};
