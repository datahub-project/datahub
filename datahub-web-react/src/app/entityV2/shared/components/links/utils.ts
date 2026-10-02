import { isFileUrl } from '@components/components/Editor/extensions/fileDragDrop';

import { GeneralizedLinkFormData, LinkFormData, LinkFormVariant } from '@app/entityV2/shared/components/links/types';

import { InstitutionalMemoryMetadata } from '@types';

export function getInitialLinkFormDataFromInstitutionMemory(
    institutionalMemoryMetadata: Partial<InstitutionalMemoryMetadata> | null | undefined,
    isDocumentationFileUploadV1Enabled?: boolean,
): Partial<LinkFormData> {
    const url = institutionalMemoryMetadata?.url;
    const label = institutionalMemoryMetadata?.label || institutionalMemoryMetadata?.description;
    const showInAssetPreview = !!institutionalMemoryMetadata?.settings?.showInAssetPreview;
    const linkType = institutionalMemoryMetadata?.linkType ?? undefined;
    const linkDescription = institutionalMemoryMetadata?.linkDescription ?? undefined;

    // Institutional memory has a link to an uploaded file
    if (isDocumentationFileUploadV1Enabled && url && isFileUrl(url)) {
        return {
            variant: LinkFormVariant.UploadFile,

            fileUrl: url,
            label,
            linkType,
            linkDescription,

            showInAssetPreview,
        };
    }

    // Institutional memory has an usual url
    return {
        variant: LinkFormVariant.URL,

        url,
        label,
        linkType,
        linkDescription,

        showInAssetPreview,
    };
}

export function getGeneralizedLinkFormDataFromFormData(data: LinkFormData): GeneralizedLinkFormData {
    if (data.variant === LinkFormVariant.UploadFile) {
        return {
            url: data.fileUrl,
            label: data.label,
            linkType: data.linkType,
            linkDescription: data.linkDescription,
            showInAssetPreview: data.showInAssetPreview,
        };
    }

    return {
        url: data.url,
        label: data.label,
        linkType: data.linkType,
        linkDescription: data.linkDescription,
        showInAssetPreview: data.showInAssetPreview,
    };
}
