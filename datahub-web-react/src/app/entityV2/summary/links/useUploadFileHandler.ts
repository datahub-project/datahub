import { notification } from '@components';
import { useCallback } from 'react';

import { validateFile } from '@components/components/Editor/extensions/fileDragDrop';
import { FileUploadFailureType } from '@components/components/Editor/types';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';

import { UploadDownloadScenario } from '@types';

export function useUploadFileHandler() {
    const { urn } = useEntityData();

    const { uploadFile: onFileUpload } = useFileUpload({
        scenario: UploadDownloadScenario.AssetDocumentationLinks,
        assetUrn: urn,
    });
    const analyticsCallbacks = useFileUploadAnalyticsCallbacks({
        scenario: UploadDownloadScenario.AssetDocumentationLinks,
        assetUrn: urn,
    });

    const handleFileUpload = useCallback(
        async (file: File) => {
            try {
                analyticsCallbacks.onFileUploadAttempt?.(file.type, file.size, 'button');

                const validation = validateFile(file);

                if (!validation.isValid) {
                    console.error(validation.error);
                    analyticsCallbacks.onFileUploadFailed?.(
                        file.type,
                        file.size,
                        'button',
                        validation.failureType || FileUploadFailureType.UNKNOWN,
                    );
                    notification.error({
                        message: 'Upload Failed',
                        description: validation.displayError || validation.error,
                    });

                    return null; // Skip invalid file
                }

                // Upload file if handler exists
                if (onFileUpload) {
                    try {
                        const finalUrl = await onFileUpload(file);
                        analyticsCallbacks.onFileUploadSucceeded?.(file.type, file.size, 'button');
                        return finalUrl;
                    } catch (uploadError) {
                        console.error(uploadError);
                        analyticsCallbacks.onFileUploadFailed?.(
                            file.type,
                            file.size,
                            'button',
                            FileUploadFailureType.UNKNOWN,
                            `${uploadError}`,
                        );
                        notification.error({
                            message: 'Upload Failed',
                            description: 'Something went wrong',
                        });
                        return null;
                    }
                }

                return null;
            } catch (error) {
                console.error(error);
                analyticsCallbacks.onFileUploadFailed?.(
                    file.type,
                    file.size,
                    'button',
                    FileUploadFailureType.UNKNOWN,
                    `${error}`,
                );
                notification.error({
                    message: 'Upload Failed',
                    description: 'Something went wrong',
                });

                return null;
            }
        },
        [analyticsCallbacks, onFileUpload],
    );

    return handleFileUpload;
}
