import { useApolloClient } from '@apollo/client';

import { useAppConfig } from '@src/app/useAppConfig';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { GetPresignedUploadUrlDocument } from '@graphql/app.generated';
import { UploadDownloadScenario } from '@types';

const PRODUCT_ASSETS_FOLDER = 'product-assets';

interface Props {
    scenario: UploadDownloadScenario;
    assetUrn?: string;
}

export default function useFileUpload({ scenario, assetUrn }: Props) {
    const client = useApolloClient();
    const { config } = useAppConfig();

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

        return resolveRuntimePath(`/openapi/v1/files/${PRODUCT_ASSETS_FOLDER}/${fileId}`);
    };

    return config.featureFlags.documentationFileUploadV1 ? { uploadFile } : { uploadFile: undefined };
}
