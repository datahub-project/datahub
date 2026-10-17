import cloudProductUpdateImage from '@images/in-product-release-oct2026.png';
import coreProductUpdateImage from '@images/sample-product-update-image.png';

const PRODUCT_UPDATE_VERSION_PATTERN = /^v?(\d+(?:\.\d+)+)$/i;
const RELEASE_MONTH_PATTERN = /^(\d{4})-(0[1-9]|1[0-2])$/;
const DATAHUB_CLOUD_RELEASE_URL_PREFIX = 'https://datahub.com/blog/datahub-cloud-';

export function getProductUpdateVersion(updateId: string): string | null {
    return PRODUCT_UPDATE_VERSION_PATTERN.exec(updateId.trim())?.[1] ?? null;
}

export function getDefaultProductUpdateLink(updateId: string, isCloud = false): string | null {
    if (!isCloud) {
        return null;
    }
    const version = getProductUpdateVersion(updateId);
    return version ? `${DATAHUB_CLOUD_RELEASE_URL_PREFIX}${version.replace(/\./g, '-')}` : null;
}

/** Image shipped in this frontend, used when the hosted payload has no image URL. */
export function bundledProductUpdateImage(isCloud: boolean): string {
    return isCloud ? cloudProductUpdateImage : coreProductUpdateImage;
}

export const PRODUCT_UPDATE_BOOTSTRAP_KEY = '__DATAHUB_PRODUCT_UPDATE__';

export type ProductUpdateBootstrap = {
    enabled?: boolean;
    id?: string;
    title?: string | null;
    header?: string | null;
    description?: string | null;
    image?: string | null;
    requiredVersion?: string | null;
    releaseMonth?: string | null;
    ctaText?: string | null;
    ctaLink?: string | null;
    primaryCtaText?: string | null;
    primaryCtaLink?: string | null;
    secondaryCtaText?: string | null;
    secondaryCtaLink?: string | null;
    features?: Array<{
        title: string;
        description: string;
        icon?: string | null;
        availability?: string | null;
    }> | null;
};

declare global {
    interface Window {
        [PRODUCT_UPDATE_BOOTSTRAP_KEY]?: ProductUpdateBootstrap | null;
    }
}

/** Hosted JSON injected into index.html by the Play server. Absent when the cache is cold. */
export function readProductUpdateBootstrap(): ProductUpdateBootstrap | null {
    if (typeof window === 'undefined') {
        return null;
    }
    const value = window[PRODUCT_UPDATE_BOOTSTRAP_KEY];
    if (!value || typeof value !== 'object' || value.enabled !== true || !value.id) {
        return null;
    }
    return value;
}

/** Prefer the hosted image URL so a JSON change can ship without a frontend build. */
export function resolveProductUpdateImage(payloadImage: string | null | undefined, isCloud: boolean): string {
    if (payloadImage && payloadImage !== 'null') {
        return payloadImage;
    }
    return bundledProductUpdateImage(isCloud);
}

export function getLocalizedReleaseMonth(locale: string, releaseMonth: string | null | undefined): string | null {
    const match = releaseMonth?.match(RELEASE_MONTH_PATTERN);
    if (!match) {
        return null;
    }

    const [, year, month] = match;
    const date = new Date(Date.UTC(Number(year), Number(month) - 1, 1));
    return new Intl.DateTimeFormat(locale, { month: 'long', timeZone: 'UTC' }).format(date);
}
