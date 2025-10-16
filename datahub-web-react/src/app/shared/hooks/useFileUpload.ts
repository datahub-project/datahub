import { useApolloClient } from '@apollo/client';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { GetPresignedUploadUrlDocument } from '@graphql/app.generated';
import { useCreateDataHubFileMutation } from '@graphql/files.generated';
import { UploadDownloadScenario } from '@types';

const PRODUCT_ASSETS_FOLDER = 'product-assets';
const FILE_ID_SEPARATOR = '__';

export interface UploadFileResult {
    fileUrl: string;
    fileId: string;
}

interface Props {
    scenario: UploadDownloadScenario;
    assetUrn?: string;
    schemaFieldUrn?: string;
}

export default function useFileUpload({ scenario, assetUrn, schemaFieldUrn }: Props) {
    const client = useApolloClient();
    const [createDataHubFile] = useCreateDataHubFileMutation();

    const uploadFile = async (file: File) => {
        const { data } = await client.query({
            query: GetPresignedUploadUrlDocument,
            variables: {
                input: {
                    scenario,
                    assetUrn,
                    contentType: file.type,
                    fileName: file.name,
                },
            },
        });

        const uploadUrl = data?.getPresignedUploadUrl.url;
        const fileId = data?.getPresignedUploadUrl.fileId;
        const filePath = data?.getPresignedUploadUrl.filePath;

        if (!uploadUrl) {
            throw new Error('Issue uploading file to server');
        }

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

        createDataHubFile({
            variables: {
                input: {
                    id: fileId.split(FILE_ID_SEPARATOR)[0],
                    storageLocation: filePath,
                    originalFileName: file.name,
                    mimeType: file.type,
                    sizeInBytes: file.size,
                    scenario,
                    referencedByAsset: assetUrn,
                    schemaField: schemaFieldUrn,
                },
            },
        });

        return resolveRuntimePath(`/api/files/${PRODUCT_ASSETS_FOLDER}/${fileId}`);
    };

    return { uploadFile };
}
