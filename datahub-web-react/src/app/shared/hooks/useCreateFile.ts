import { useCallback } from 'react';

import { PRODUCT_ASSETS_FOLDER } from '@app/shared/constants';

import { useCreateDataHubFileMutation } from '@graphql/app.generated';
import { UploadDownloadScenario } from '@types';

// keep this consistent with same const in li-utils/src/main/java/com/linkedin/metadata/Constants.java
export const S3_FILE_ID_NAME_SEPARATOR = '__';
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 300;

// Helper function to read a file as an ArrayBuffer
// Read via FileReader (jsdom supports this better)
const readFileAsArrayBuffer = (file: File): Promise<ArrayBuffer> => {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result as ArrayBuffer);
        reader.onerror = () => reject(reader.error);
        reader.readAsArrayBuffer(file);
    });
};

// Utility function to calculate SHA-256 hash of file content
const calculateSHA256Hash = async (file: File): Promise<string> => {
    const arrayBuffer = await readFileAsArrayBuffer(file);

    const hashBuffer = await crypto.subtle.digest('SHA-256', arrayBuffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
};

interface Props {
    scenario: UploadDownloadScenario;
    assetUrn?: string;
    schemaField?: string;
}

export default function useCreateFile({ scenario, assetUrn, schemaField }: Props) {
    const [createFileMutation] = useCreateDataHubFileMutation();

    const createFile = useCallback(
        async (fileId: string, file: File) => {
            const contentHash = await calculateSHA256Hash(file);

            const create = async (attempt = 1) => {
                try {
                    const response = await createFileMutation({
                        variables: {
                            input: {
                                id: fileId,
                                mimeType: file.type,
                                originalFileName: file.name,
                                referencedByAsset: assetUrn,
                                schemaField,
                                scenario,
                                sizeInBytes: file.size,
                                storageKey: `${PRODUCT_ASSETS_FOLDER}/${fileId}${S3_FILE_ID_NAME_SEPARATOR}${file.name}`,
                                contentHash,
                            },
                        },
                    });

                    if (!response.data?.createDataHubFile?.file?.urn) {
                        throw new Error(`Failed to upload file: ${JSON.stringify(response.errors)}`);
                    }
                    return response.data.createDataHubFile.file.urn;
                } catch (error) {
                    if (attempt < MAX_RETRIES) {
                        // eslint-disable-next-line no-promise-executor-return
                        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
                        return create(attempt + 1);
                    }
                    throw new Error(`Failed to upload file after ${MAX_RETRIES} attempts: ${error}`);
                }
            };

            return create();
        },
        [scenario, assetUrn, schemaField, createFileMutation],
    );

    return { createFile };
}
