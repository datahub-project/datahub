/**
 * Utility functions for handling file uploads to S3 with pre-signed URLs
 */

interface PreSignedUrlResponse {
    uploadUrl: string;
    fileUrl: string;
}

/**
 * Mock function to get a pre-signed URL from the backend
 * In a real implementation, this would call your backend API
 */
export const getPreSignedUrl = async (fileName: string, fileType: string): Promise<PreSignedUrlResponse> => {
    // TODO: Replace with actual API call to your backend
    // Example endpoint: POST /api/files/presign-upload

    try {
        const response = await fetch('/api/files/presign-upload', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                // Add any auth headers your API requires
            },
            body: JSON.stringify({
                fileName,
                fileType,
            }),
        });

        if (!response.ok) {
            throw new Error(`Failed to get pre-signed URL: ${response.statusText}`);
        }

        const data = await response.json();
        return {
            uploadUrl: data.uploadUrl,
            fileUrl: data.fileUrl,
        };
    } catch (error) {
        console.error('Error getting pre-signed URL:', error);

        // For POC purposes, return mock URLs
        // In production, you should handle this error appropriately
        const mockFileId = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        return {
            uploadUrl: `https://mock-upload-url.com/${mockFileId}`,
            fileUrl: `https://mock-file-url.com/${mockFileId}/${fileName}`,
        };
    }
};

/**
 * Upload a file to S3 using a pre-signed URL
 */
export const uploadFileToS3 = async (file: File, uploadUrl: string): Promise<void> => {
    try {
        const response = await fetch(uploadUrl, {
            method: 'PUT',
            body: file,
            headers: {
                'Content-Type': file.type,
            },
        });

        if (!response.ok) {
            throw new Error(`Failed to upload file: ${response.statusText}`);
        }
    } catch (error) {
        console.error('Error uploading file to S3:', error);
        throw error;
    }
};

/**
 * Complete file upload process: get pre-signed URL and upload file
 */
export const uploadFile = async (file: File): Promise<string> => {
    try {
        // Step 1: Get pre-signed URL from backend
        const { uploadUrl, fileUrl } = await getPreSignedUrl(file.name, file.type);

        // Step 2: Upload file to S3 using pre-signed URL
        await uploadFileToS3(file, uploadUrl);

        // Step 3: Return the final file URL
        return fileUrl;
    } catch (error) {
        console.error('Error in complete upload process:', error);

        // For POC purposes, return a mock URL if upload fails
        // In production, you should handle this error appropriately
        const mockFileId = `mock-${Date.now()}-${Math.random().toString(36).substring(7)}`;
        return `https://mock-file-storage.com/${mockFileId}/${file.name}`;
    }
};

/**
 * Validate file before upload
 */
export const validateFile = (
    file: File,
    options?: {
        maxSize?: number; // in bytes
        allowedTypes?: string[];
    },
): { isValid: boolean; error?: string } => {
    const { maxSize = 10 * 1024 * 1024, allowedTypes } = options || {}; // Default 10MB

    // Check file size
    if (file.size > maxSize) {
        return {
            isValid: false,
            error: `File size (${(file.size / 1024 / 1024).toFixed(2)}MB) exceeds maximum allowed size (${(maxSize / 1024 / 1024).toFixed(2)}MB)`,
        };
    }

    // Check file type if specified
    if (allowedTypes && !allowedTypes.includes(file.type)) {
        return {
            isValid: false,
            error: `File type "${file.type}" is not allowed. Supported types: ${allowedTypes.join(', ')}`,
        };
    }

    return { isValid: true };
};
