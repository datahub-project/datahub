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

export function getExternalUrlCandidates(externalUrl: string): string[] {
    const canonicalUrl = getCanonicalBigQueryUrl(externalUrl);
    return canonicalUrl && canonicalUrl !== externalUrl ? [externalUrl, canonicalUrl] : [externalUrl];
}
