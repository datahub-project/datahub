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

/**
 * Image shipped in this frontend. The product-update payload's image URL is not used:
 * it arrives only after GraphQL, and a remote URL then becomes the home LCP even though
 * this build already contains the file.
 */
export function bundledProductUpdateImage(isCloud: boolean): string {
    return isCloud ? cloudProductUpdateImage : coreProductUpdateImage;
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
