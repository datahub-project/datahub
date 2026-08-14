import { getExternalUrlCandidates, getExternalUrlContainTokens } from '@app/embed/lookup/utils';

const WORKSPACE_REPORT_URL =
    'https://app.powerbi.com/groups/11111111-1111-1111-1111-111111111111/reports/22222222-2222-2222-2222-222222222222';
const APP_REPORT_URL =
    'https://app.powerbi.com/groups/me/apps/33333333-3333-3333-3333-333333333333/reports/22222222-2222-2222-2222-222222222222/44444444444444444444444444444444?experience=power-bi';

describe('getExternalUrlCandidates', () => {
    it('adds Power BI candidates without query params and page segments', () => {
        expect(getExternalUrlCandidates(APP_REPORT_URL)).toEqual([
            APP_REPORT_URL,
            'https://app.powerbi.com/groups/me/apps/33333333-3333-3333-3333-333333333333/reports/22222222-2222-2222-2222-222222222222/44444444444444444444444444444444',
            'https://app.powerbi.com/groups/me/apps/33333333-3333-3333-3333-333333333333/reports/22222222-2222-2222-2222-222222222222',
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
        expect(getExternalUrlContainTokens(APP_REPORT_URL)).toEqual(['/reports/22222222-2222-2222-2222-222222222222']);
    });

    it('returns a report path token for workspace Power BI URLs', () => {
        expect(getExternalUrlContainTokens(WORKSPACE_REPORT_URL)).toEqual([
            '/reports/22222222-2222-2222-2222-222222222222',
        ]);
    });

    it('returns a dashboard path token for Power BI dashboards', () => {
        const dashboardUrl =
            'https://app.powerbi.com/groups/me/apps/33333333-3333-3333-3333-333333333333/dashboards/66666666-6666-6666-6666-666666666666';

        expect(getExternalUrlContainTokens(dashboardUrl)).toEqual(['/dashboards/66666666-6666-6666-6666-666666666666']);
    });

    it('supports Power BI gov cloud hosts', () => {
        const govUrl =
            'https://app.powerbigov.us/groups/me/apps/33333333-3333-3333-3333-333333333333/reports/22222222-2222-2222-2222-222222222222';

        expect(getExternalUrlContainTokens(govUrl)).toEqual(['/reports/22222222-2222-2222-2222-222222222222']);
    });

    it('returns no tokens for non-Power BI URLs', () => {
        expect(getExternalUrlContainTokens('https://example.com/reports/abc')).toEqual([]);
    });
});
