const BIGQUERY_HOSTNAME = 'console.cloud.google.com';
const BIGQUERY_PATHNAME = '/bigquery';
const BIGQUERY_TABLE_TARGET_PATTERN = /!1s([^!]+)!2s([^!]+)!3s([^!]+)/;
const BIGQUERY_DATASET_TARGET_PATTERN = /!1s([^!]+)!2s([^!]+)/;

function getCanonicalBigQueryUrl(externalUrl: string): string | null {
    let url: URL;
    try {
        url = new URL(externalUrl);
    } catch {
        return null;
    }

    if (url.hostname !== BIGQUERY_HOSTNAME || url.pathname !== BIGQUERY_PATHNAME) return null;

    const workspace = url.searchParams.get('ws');
    if (!workspace) return null;

    const tableTarget = workspace.match(BIGQUERY_TABLE_TARGET_PATTERN);
    if (tableTarget) {
        const [, project, dataset, table] = tableTarget;
        return `https://${BIGQUERY_HOSTNAME}${BIGQUERY_PATHNAME}?project=${project}&ws=!1m5!1m4!4m3!1s${project}!2s${dataset}!3s${table}`;
    }

    const datasetTarget = workspace.match(BIGQUERY_DATASET_TARGET_PATTERN);
    if (datasetTarget) {
        const [, project, dataset] = datasetTarget;
        return `https://${BIGQUERY_HOSTNAME}${BIGQUERY_PATHNAME}?project=${project}&ws=!1m4!1m3!3m2!1s${project}!2s${dataset}`;
    }

    return null;
}

function getBigQueryEqualCandidates(externalUrl: string): string[] {
    const canonicalUrl = getCanonicalBigQueryUrl(externalUrl);
    return canonicalUrl && canonicalUrl !== externalUrl ? [canonicalUrl] : [];
}

// Power BI is reachable from several sovereign clouds, and Fabric now fronts the same
// artifacts. Ingestion only ever writes app.powerbi.com / app.powerbigov.us, but the
// browser can be on any of these, so the token below is what bridges the two.
const POWERBI_HOST_PATTERN = /^app\.(powerbi\.(com|cn)|(high\.|mil\.)?powerbigov\.us|fabric\.microsoft\.com)$/i;

const POWERBI_ENTITY_TYPES = new Set(['reports', 'dashboards', 'rdlreports']);

// Embed and deep-link URLs (e.g. /reportEmbed) carry the artifact id in the query string
// rather than the path.
const POWERBI_QUERY_ID_PARAMS: Record<string, string> = {
    reportId: 'reports',
    dashboardId: 'dashboards',
};

const GUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const WORKSPACE_SEGMENT = 'groups';
const APP_SEGMENT = 'apps';
const GROUP_ID_PARAM = 'groupId';

type PowerBiEntityRef = {
    entityType: string;
    entityId: string;
    // Workspace-scoped path matching what ingestion stores, when the browser URL carries
    // enough information to rebuild it. App URLs identify the workspace only by app id,
    // so there is nothing to rebuild from.
    canonicalPathname: string | null;
    idInQuery: boolean;
};

function isGuid(value: string | undefined): value is string {
    return !!value && GUID_PATTERN.test(value);
}

function getRefFromPath(url: URL): PowerBiEntityRef | null {
    const pathParts = url.pathname.split('/').filter(Boolean);
    if (pathParts[0]?.toLowerCase() !== WORKSPACE_SEGMENT) {
        return null;
    }

    // Expected shapes:
    // /groups/{workspaceId}/{reports|dashboards|rdlreports}/{id}
    // /groups/me/apps/{appId}/{reports|dashboards|rdlreports}/{id}
    const entityTypeIndex = pathParts.findIndex((part) => POWERBI_ENTITY_TYPES.has(part.toLowerCase()));
    if (entityTypeIndex < 1) {
        return null;
    }

    const entityType = pathParts[entityTypeIndex]?.toLowerCase();
    const entityId = pathParts[entityTypeIndex + 1];
    if (!entityType || !isGuid(entityId)) {
        return null;
    }

    const isAppScoped = pathParts.slice(0, entityTypeIndex).some((part) => part.toLowerCase() === APP_SEGMENT);

    return {
        entityType,
        entityId,
        canonicalPathname: isAppScoped ? null : `/${pathParts.slice(0, entityTypeIndex + 2).join('/')}`,
        idInQuery: false,
    };
}

function getRefFromQuery(url: URL): PowerBiEntityRef | null {
    const match = Object.entries(POWERBI_QUERY_ID_PARAMS)
        .map(([param, entityType]) => ({ entityType, entityId: url.searchParams.get(param) ?? undefined }))
        .find((candidate) => isGuid(candidate.entityId));

    if (!match || !isGuid(match.entityId)) {
        return null;
    }

    const groupId = url.searchParams.get(GROUP_ID_PARAM) ?? undefined;

    return {
        entityType: match.entityType,
        entityId: match.entityId,
        canonicalPathname: isGuid(groupId)
            ? `/${WORKSPACE_SEGMENT}/${groupId}/${match.entityType}/${match.entityId}`
            : null,
        idInQuery: true,
    };
}

function getPowerBiRef(externalUrl: string): { url: URL; ref: PowerBiEntityRef } | null {
    let url: URL;
    try {
        url = new URL(externalUrl);
    } catch {
        return null;
    }

    if (!POWERBI_HOST_PATTERN.test(url.hostname)) {
        return null;
    }

    const ref = getRefFromPath(url) ?? getRefFromQuery(url);
    return ref ? { url, ref } : null;
}

function getPowerBiEqualCandidates(externalUrl: string): string[] {
    const parsed = getPowerBiRef(externalUrl);
    if (!parsed) {
        return [];
    }

    const { url, ref } = parsed;
    const candidates = new Set<string>();

    // Dropping the query string would discard the id itself when it lives there.
    if (!ref.idInQuery) {
        candidates.add(`${url.origin}${url.pathname}`);
    }

    if (ref.canonicalPathname) {
        candidates.add(`${url.origin}${ref.canonicalPathname}`);
    }

    candidates.delete(externalUrl);
    return [...candidates];
}

function getPowerBiContainTokens(externalUrl: string): string[] {
    const parsed = getPowerBiRef(externalUrl);
    if (!parsed) {
        return [];
    }

    // Report and dashboard ids are globally unique, so this token identifies the artifact
    // even when the workspace GUID or host differs from what ingestion stored.
    return [`/${parsed.ref.entityType}/${parsed.ref.entityId}`];
}

/**
 * Exact-match URL variants to try during embed lookup.
 * Always includes the original URL; may include normalized variants for known platforms.
 */
export function getExternalUrlCandidates(externalUrl: string): string[] {
    const candidates = [
        externalUrl,
        ...getBigQueryEqualCandidates(externalUrl),
        ...getPowerBiEqualCandidates(externalUrl),
    ];
    return [...new Set(candidates)];
}

/**
 * Substring tokens used as a fallback when exact URL match finds nothing
 * (e.g. Power BI Workspace App URLs vs stored workspace report URLs).
 */
export function getExternalUrlContainTokens(externalUrl: string): string[] {
    return getPowerBiContainTokens(externalUrl);
}
