import { getExternalUrlCandidates } from '@app/embed/lookup/utils';

const CANONICAL_TABLE_URL =
    'https://console.cloud.google.com/bigquery?project=example-project&ws=!1m5!1m4!4m3!1sexample-project!2sanalytics!3sevents';

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

    it('leaves URLs from other platforms unchanged', () => {
        const externalUrl = 'https://example.com/dashboards/123';

        expect(getExternalUrlCandidates(externalUrl)).toEqual([externalUrl]);
    });
});
