export enum LinkFormVariant {
    URL = 'url',
    UploadFile = 'uploadFile',
}

export interface LinkFormData {
    variant: LinkFormVariant;
    url: string;
    fileUrl: string;

    label: string;
    linkType?: string;
    linkDescription?: string;

    showInAssetPreview: boolean;
}

export interface GeneralizedLinkFormData {
    url: string;
    label: string;
    linkType?: string;
    linkDescription?: string;

    showInAssetPreview: boolean;
}
