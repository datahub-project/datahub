import { useApolloClient } from '@apollo/client';

import { PRODUCT_ASSETS_FOLDER } from '@app/shared/constants';
import useCreateFile from '@app/shared/hooks/useCreateFile';
import { useAppConfig } from '@src/app/useAppConfig';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { GetPresignedUploadUrlDocument } from '@graphql/app.generated';
import { UploadDownloadScenario } from '@types';

// keep this consistent with same const in li-utils/src/main/java/com/linkedin/metadata/Constants.java
const S3_FILE_ID_NAME_SEPARATOR = '__';

interface Props {
    scenario: UploadDownloadScenario;
    assetUrn?: string;
    schemaField?: string;
}

export default function useFileUpload({ scenario, assetUrn, schemaField }: Props) {
    const client = useApolloClient();
    const { config } = useAppConfig();
    const { createFile } = useCreateFile({ scenario, assetUrn, schemaField });

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

        const uploadUrl = data?.getPresignedUploadUrl?.url;
        const fileId = data?.getPresignedUploadUrl?.fileId;

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

        // Confirming of file uploading
        try {
            const uuidFromFileId = fileId.split(S3_FILE_ID_NAME_SEPARATOR)[0];
            await createFile(uuidFromFileId, file);
        } catch (error) {
            throw new Error(`Failed to upload file: ${error}`);
        }

        return resolveRuntimePath(`/openapi/v1/files/${PRODUCT_ASSETS_FOLDER}/${fileId}`);
    };

    return config.featureFlags.documentationFileUploadV1 ? { uploadFile } : { uploadFile: undefined };
}
