import { useCallback } from 'react';

import { useCreateDataHubFileMutation } from '@graphql/app.generated';
import { UploadDownloadScenario } from '@types';

import { PRODUCT_ASSETS_FOLDER } from '../constants';

interface Props {
    scenario: UploadDownloadScenario;
    assetUrn?: string;
    schemaField?: string;
}

export default function useCreateFile({ scenario, assetUrn, schemaField }: Props) {
    const [createFileMutation] = useCreateDataHubFileMutation();

    const createFile = useCallback(
        async (fileId: string, file: File) => {
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
                            storageBucket: 'test', // TODO: Replace with proper bucket configuration from app config
                            storageKey: `${PRODUCT_ASSETS_FOLDER}/${fileId}`,
                        },
                    },
                });

                if (!response.data?.createDataHubFile?.file?.urn) {
                    throw new Error(`Failed to upload file: ${JSON.stringify(response.errors)}`);
                }
            } catch (error) {
                throw new Error(`Failed to upload file: ${error}`);
            }
        },
        [scenario, assetUrn, schemaField, createFileMutation],
    );


    return { createFile };
}
