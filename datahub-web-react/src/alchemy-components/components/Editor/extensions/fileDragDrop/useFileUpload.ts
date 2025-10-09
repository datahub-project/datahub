import { useApolloClient } from '@apollo/client';

import { GetPresignedUploadUrlDocument } from '@graphql/app.generated';
import { UploadDownloadScenario } from '@types';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

export default function useFileUpload() {
    const client = useApolloClient();

    const uploadFile = async (file: File) => {
        const { data } = await client.query({
            query: GetPresignedUploadUrlDocument,
            variables: {
                input: {
                    scenario: UploadDownloadScenario.AssetDocumentation,
                    assetUrn: 'urn:li:glossaryNode:c21f8d1a-a2d6-4712-b363-cdd1a99f6c76',
                    contentType: file.type,
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

        return resolveRuntimePath(`/api/files/${fileId}`);
    };

    return { uploadFile };
}
