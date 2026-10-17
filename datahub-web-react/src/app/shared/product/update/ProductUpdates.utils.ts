import { resolveRuntimePath } from '@utils/runtimeBasePath';

const PRODUCT_UPDATE_VERSION_PATTERN = /^v?(\d+(?:\.\d+)+)$/i;
const RELEASE_MONTH_PATTERN = /^(\d{4})-(0[1-9]|1[0-2])$/;
const DATAHUB_CLOUD_RELEASE_URL_PREFIX = 'https://datahub.com/blog/datahub-cloud-';

// Vite copies src/images/* to assets/platforms/. The toast JSON still names some of those
// files by their GitHub raw URL, which the browser then loads from githubusercontent.com.
const BUNDLED_GITHUB_IMAGE = new RegExp(
    '^https://raw\\.githubusercontent\\.com/datahub-project/datahub/[^/]+/' +
        'datahub-web-react/src/images/([A-Za-z0-9][A-Za-z0-9._-]*)$',
);
const LOCAL_PLATFORM_IMAGE = /^(?:\/)?assets\/platforms\/([A-Za-z0-9][A-Za-z0-9._-]*)$/;

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
 * Serve a product-update image from the app build when the file ships in src/images.
 * External URLs that are not a bundled repo image are left unchanged.
 */
export function resolveProductUpdateImage(
    image: string | null | undefined,
    resolvePath: (path: string) => string = resolveRuntimePath,
): string | null {
    if (!image) {
        return null;
    }
    const filename = BUNDLED_GITHUB_IMAGE.exec(image)?.[1] ?? LOCAL_PLATFORM_IMAGE.exec(image)?.[1];
    if (!filename) {
        return image;
    }
    return resolvePath(`/assets/platforms/${filename}`);
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
