import { resolveRuntimePath } from '@utils/runtimeBasePath';

const PRODUCT_UPDATE_VERSION_PATTERN = /^v?(\d+(?:\.\d+)+)$/i;
const RELEASE_MONTH_PATTERN = /^(\d{4})-(0[1-9]|1[0-2])$/;
const DATAHUB_CLOUD_RELEASE_URL_PREFIX = 'https://datahub.com/blog/datahub-cloud-';

/** Vite copies `datahub-web-react/src/images/*` to this directory. Product-update JSON uses the same path. */
export const PRODUCT_UPDATE_IMAGE_ROOT = 'assets/platforms/';

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
 * Anchor a product-update image under PRODUCT_UPDATE_IMAGE_ROOT.
 * An http(s) URL is already absolute and is left unchanged.
 */
export function resolveProductUpdateImage(
    image: string | null | undefined,
    resolvePath: (path: string) => string = resolveRuntimePath,
): string | null {
    if (!image) {
        return null;
    }
    if (image.startsWith('http://') || image.startsWith('https://')) {
        return image;
    }
    const relative = image.startsWith('/') ? image.slice(1) : image;
    if (!relative.startsWith(PRODUCT_UPDATE_IMAGE_ROOT)) {
        return image;
    }
    return resolvePath(`/${relative}`);
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
