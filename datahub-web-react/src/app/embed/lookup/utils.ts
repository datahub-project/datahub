const POWERBI_HOST_PATTERN = /^app\.powerbi(gov)?\.(com|us)$/i;
const POWERBI_ENTITY_TYPES = new Set(['reports', 'dashboards', 'rdlreports']);

type PowerBiEntityRef = {
    entityType: string;
    entityId: string;
    pathnameWithoutPage: string;
};

function getPowerBiEntityRef(externalUrl: string): PowerBiEntityRef | null {
    let url: URL;
    try {
        url = new URL(externalUrl);
    } catch {
        return null;
    }

    if (!POWERBI_HOST_PATTERN.test(url.hostname)) {
        return null;
    }

    const pathParts = url.pathname.split('/').filter(Boolean);
    const entityTypeIndex = pathParts.findIndex((part) => POWERBI_ENTITY_TYPES.has(part.toLowerCase()));
    if (entityTypeIndex < 0 || entityTypeIndex + 1 >= pathParts.length) {
        return null;
    }

    // Expected prefixes:
    // /groups/{workspaceId}/reports|{dashboards|rdlreports}/{id}
    // /groups/me/apps/{appId}/reports|{dashboards|rdlreports}/{id}
    if (pathParts[0]?.toLowerCase() !== 'groups' || pathParts.length < entityTypeIndex + 2) {
        return null;
    }

    const entityType = pathParts[entityTypeIndex];
    const entityId = pathParts[entityTypeIndex + 1];
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(entityId)) {
        return null;
    }

    return {
        entityType: entityType.toLowerCase(),
        entityId,
        pathnameWithoutPage: `/${pathParts.slice(0, entityTypeIndex + 2).join('/')}`,
    };
}

function getPowerBiEqualCandidates(externalUrl: string): string[] {
    const ref = getPowerBiEntityRef(externalUrl);
    if (!ref) {
        return [];
    }

    let url: URL;
    try {
        url = new URL(externalUrl);
    } catch {
        return [];
    }

    const candidates = new Set<string>();
    const withoutQuery = `${url.origin}${url.pathname}`;
    if (withoutQuery !== externalUrl) {
        candidates.add(withoutQuery);
    }

    const withoutPage = `${url.origin}${ref.pathnameWithoutPage}`;
    if (withoutPage !== externalUrl) {
        candidates.add(withoutPage);
    }

    return [...candidates];
}

function getPowerBiContainTokens(externalUrl: string): string[] {
    const ref = getPowerBiEntityRef(externalUrl);
    if (!ref) {
        return [];
    }
    // Match ingestion-stored workspace URLs even when the browser is on an App URL,
    // which does not include the workspace GUID needed to rebuild the exact URL.
    return [`/${ref.entityType}/${ref.entityId}`];
}

/**
 * Exact-match URL variants to try during embed lookup.
 * Always includes the original URL; may include normalized variants for known platforms.
 */
export function getExternalUrlCandidates(externalUrl: string): string[] {
    const candidates = [externalUrl, ...getPowerBiEqualCandidates(externalUrl)];
    return [...new Set(candidates)];
}

/**
 * Substring tokens for CONTAIN filters when exact URL match is impossible
 * (e.g. Power BI Workspace App URLs vs stored workspace report URLs).
 */
export function getExternalUrlContainTokens(externalUrl: string): string[] {
    return getPowerBiContainTokens(externalUrl);
}
