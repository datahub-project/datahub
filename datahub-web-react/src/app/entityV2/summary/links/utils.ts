import { isFileUrl } from '@components/components/Editor/extensions/fileDragDrop';

import { GeneralizedLinkFormData, LinkFormData, LinkFormVariant } from '@app/entityV2/summary/links/types';

import { InstitutionalMemoryMetadata } from '@types';

export function getInitialLinkFormDataFromInstitutionMemory(
    institutionalMemoryMetadata: Partial<InstitutionalMemoryMetadata> | null | undefined,
    isDocumentationFileUploadV1Enabled?: boolean,
): Partial<LinkFormData> {
    const url = institutionalMemoryMetadata?.url;
    const label = institutionalMemoryMetadata?.label || institutionalMemoryMetadata?.description;
    const showInAssetPreview = !!institutionalMemoryMetadata?.settings?.showInAssetPreview;

    // Institutional memory has a link to an uploaded file
    if (isDocumentationFileUploadV1Enabled && url && isFileUrl(url)) {
        return {
            variant: LinkFormVariant.UploadFile,

            fileUrl: url,
            label,

            showInAssetPreview,
        };
    }

    // Institutional memory has an usual url
    return {
        variant: LinkFormVariant.URL,

        url,
        label,

        showInAssetPreview,
    };
}

export function getGeneralizedLinkFormDataFromFormData(data: LinkFormData): GeneralizedLinkFormData {
    if (data.variant === LinkFormVariant.UploadFile) {
        return {
            url: data.fileUrl,
            label: data.label,
            showInAssetPreview: data.showInAssetPreview,
        };
    }

    return {
        url: data.url,
        label: data.label,
        showInAssetPreview: data.showInAssetPreview,
    };
}
