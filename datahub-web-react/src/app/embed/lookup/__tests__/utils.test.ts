import { getExternalUrlCandidates, getExternalUrlContainTokens } from '@app/embed/lookup/utils';

const CANONICAL_TABLE_URL =
    'https://console.cloud.google.com/bigquery?project=example-project&ws=!1m5!1m4!4m3!1sexample-project!2sanalytics!3sevents';

const WORKSPACE_ID = '11111111-1111-1111-1111-111111111111';
const REPORT_ID = '22222222-2222-2222-2222-222222222222';
const APP_ID = '33333333-3333-3333-3333-333333333333';
const PAGE_ID = '44444444444444444444444444444444';
const DASHBOARD_ID = '66666666-6666-6666-6666-666666666666';

const WORKSPACE_REPORT_URL = `https://app.powerbi.com/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`;
const APP_REPORT_URL = `https://app.powerbi.com/groups/me/apps/${APP_ID}/reports/${REPORT_ID}/${PAGE_ID}?experience=power-bi`;

const REPORT_TOKEN = `/reports/${REPORT_ID}`;

describe('getExternalUrlCandidates', () => {
    it.each(['WS_URL_PARAM', 'RESOURCE_LIST'])('adds the canonical BigQuery table URL for the %s view', (view) => {
        const rewrittenUrl =
            `https://console.cloud.google.com/bigquery?project=example-project&ws=` +
            `!1m6!1m5!4m3!1sexample-project!2sanalytics!3sevents!23s${view}`;

        expect(getExternalUrlCandidates(rewrittenUrl)).toEqual([rewrittenUrl, CANONICAL_TABLE_URL]);
    });

    it('uses the target project when the selected Google Cloud project is different', () => {
        const rewrittenUrl =
            'https://console.cloud.google.com/bigquery?project=billing-project&ws=' +
            '!1m6!1m5!4m3!1sexample-project!2sanalytics!3sevents!23sWS_URL_PARAM';

        expect(getExternalUrlCandidates(rewrittenUrl)).toEqual([rewrittenUrl, CANONICAL_TABLE_URL]);
    });

    it('does not duplicate a canonical BigQuery URL', () => {
        expect(getExternalUrlCandidates(CANONICAL_TABLE_URL)).toEqual([CANONICAL_TABLE_URL]);
    });

    it('adds the canonical BigQuery dataset URL', () => {
        const rewrittenUrl =
            'https://console.cloud.google.com/bigquery?project=example-project&ws=' +
            '!1m5!1m4!3m2!1sexample-project!2sanalytics!23sRESOURCE_LIST';
        const canonicalUrl =
            'https://console.cloud.google.com/bigquery?project=example-project&ws=' +
            '!1m4!1m3!3m2!1sexample-project!2sanalytics';

        expect(getExternalUrlCandidates(rewrittenUrl)).toEqual([rewrittenUrl, canonicalUrl]);
    });

    it('strips the query string and page segment from a Workspace App URL', () => {
        expect(getExternalUrlCandidates(APP_REPORT_URL)).toEqual([
            APP_REPORT_URL,
            `https://app.powerbi.com/groups/me/apps/${APP_ID}/reports/${REPORT_ID}/${PAGE_ID}`,
        ]);
    });

    it('adds a workspace Power BI report candidate when a page id is present', () => {
        const withPage = `${WORKSPACE_REPORT_URL}/55555555555555555555555555555555?experience=power-bi`;

        expect(getExternalUrlCandidates(withPage)).toEqual([
            withPage,
            `${WORKSPACE_REPORT_URL}/55555555555555555555555555555555`,
            WORKSPACE_REPORT_URL,
        ]);
    });

    it('rebuilds a workspace URL from an embed URL that carries a group id', () => {
        const embedUrl = `https://app.powerbi.com/reportEmbed?reportId=${REPORT_ID}&groupId=${WORKSPACE_ID}&autoAuth=true`;

        expect(getExternalUrlCandidates(embedUrl)).toEqual([embedUrl, WORKSPACE_REPORT_URL]);
    });

    it('does not strip the query string when the id only lives there', () => {
        const embedUrl = `https://app.powerbi.com/reportEmbed?reportId=${REPORT_ID}`;

        expect(getExternalUrlCandidates(embedUrl)).toEqual([embedUrl]);
    });

    it('does not duplicate an already-canonical Power BI URL', () => {
        expect(getExternalUrlCandidates(WORKSPACE_REPORT_URL)).toEqual([WORKSPACE_REPORT_URL]);
    });

    it('leaves URLs from other platforms unchanged', () => {
        const externalUrl = 'https://example.com/dashboards/123';

        expect(getExternalUrlCandidates(externalUrl)).toEqual([externalUrl]);
    });
});

describe('getExternalUrlContainTokens', () => {
    it('returns a report path token for Power BI Workspace App URLs', () => {
        expect(getExternalUrlContainTokens(APP_REPORT_URL)).toEqual([REPORT_TOKEN]);
    });

    it('returns a report path token for workspace Power BI URLs', () => {
        expect(getExternalUrlContainTokens(WORKSPACE_REPORT_URL)).toEqual([REPORT_TOKEN]);
    });

    it('returns a dashboard path token for Power BI dashboards', () => {
        const dashboardUrl = `https://app.powerbi.com/groups/me/apps/${APP_ID}/dashboards/${DASHBOARD_ID}`;

        expect(getExternalUrlContainTokens(dashboardUrl)).toEqual([`/dashboards/${DASHBOARD_ID}`]);
    });

    it('returns a token for paginated reports', () => {
        const rdlUrl = `https://app.powerbi.com/groups/${WORKSPACE_ID}/rdlreports/${REPORT_ID}`;

        expect(getExternalUrlContainTokens(rdlUrl)).toEqual([`/rdlreports/${REPORT_ID}`]);
    });

    it('returns a token for ids carried in the query string', () => {
        const embedUrl = `https://app.powerbi.com/reportEmbed?reportId=${REPORT_ID}&autoAuth=true`;

        expect(getExternalUrlContainTokens(embedUrl)).toEqual([REPORT_TOKEN]);
    });

    it.each([
        ['gov cloud', 'app.powerbigov.us'],
        ['gov high cloud', 'app.high.powerbigov.us'],
        ['dod cloud', 'app.mil.powerbigov.us'],
        ['china cloud', 'app.powerbi.cn'],
        ['fabric', 'app.fabric.microsoft.com'],
    ])('supports Power BI on %s', (_name, host) => {
        const url = `https://${host}/groups/me/apps/${APP_ID}/reports/${REPORT_ID}`;

        expect(getExternalUrlContainTokens(url)).toEqual([REPORT_TOKEN]);
    });

    it('ignores hosts that only look like Power BI', () => {
        const lookalike = `https://app.powerbi.com.evil.example/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`;

        expect(getExternalUrlContainTokens(lookalike)).toEqual([]);
    });

    it('ignores non-GUID ids so wildcard patterns cannot be injected', () => {
        const wildcardUrl = 'https://app.powerbi.com/groups/me/apps/abc/reports/*';

        expect(getExternalUrlContainTokens(wildcardUrl)).toEqual([]);
    });

    it('returns no tokens for non-Power BI URLs', () => {
        expect(getExternalUrlContainTokens('https://example.com/reports/abc')).toEqual([]);
    });
});
