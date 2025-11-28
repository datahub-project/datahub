import { useAppConfig } from '@app/useAppConfig';

export function useIsDocumentationFileUploadV1Enabled() {
    const { config } = useAppConfig();
    return config.featureFlags.documentationFileUploadV1;
}
