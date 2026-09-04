const PRODUCT_UPDATE_VERSION_PATTERN = /^v?(\d+(?:\.\d+)+)$/i;
const DATAHUB_CLOUD_RELEASE_URL_PREFIX = 'https://datahub.com/blog/datahub-cloud-';

export function getProductUpdateVersion(updateId: string): string | null {
    return PRODUCT_UPDATE_VERSION_PATTERN.exec(updateId.trim())?.[1] ?? null;
}

export function getDefaultProductUpdateLink(updateId: string): string | null {
    const version = getProductUpdateVersion(updateId);
    return version ? `${DATAHUB_CLOUD_RELEASE_URL_PREFIX}${version.replace(/\./g, '-')}` : null;
}

export function getLocalizedCurrentMonth(locale: string, now = new Date()): string {
    return new Intl.DateTimeFormat(locale, { month: 'long' }).format(now);
}
