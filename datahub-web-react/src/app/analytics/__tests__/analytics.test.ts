import { THIRD_PARTY_LOGGING_KEY, buildEventContext, getMergedTrackingOptions } from '@app/analytics/analytics';

describe('buildEventContext', () => {
    afterEach(() => {
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
        localStorage.removeItem('i18nEnabled');
        localStorage.removeItem('userLanguage');
    });

    it('captures the browser locale so analytics can report language demand', () => {
        vi.spyOn(navigator, 'language', 'get').mockReturnValue('de-DE');

        expect(buildEventContext().language).toEqual('de-DE');
    });

    it('captures the served UI locale separately from the browser locale', () => {
        localStorage.setItem('i18nEnabled', 'true');
        localStorage.setItem('userLanguage', 'null');
        vi.stubGlobal('navigator', {
            language: 'de-DE',
            languages: ['de-DE', 'de'],
            userAgent: 'test-agent',
        });

        expect(buildEventContext()).toMatchObject({
            language: 'de-DE',
            appLanguage: 'de',
        });
    });

    it('uses an explicit in-app language preference as the served locale', () => {
        localStorage.setItem('i18nEnabled', 'true');
        localStorage.setItem('userLanguage', '"pt-BR"');
        vi.stubGlobal('navigator', {
            language: 'de-DE',
            languages: ['de-DE'],
            userAgent: 'test-agent',
        });

        expect(buildEventContext().appLanguage).toEqual('pt-BR');
    });

    it('falls back to English when i18n is disabled', () => {
        localStorage.setItem('i18nEnabled', 'false');
        localStorage.setItem('userLanguage', '"de"');
        vi.stubGlobal('navigator', {
            language: 'de-DE',
            languages: ['de-DE'],
            userAgent: 'test-agent',
        });

        expect(buildEventContext().appLanguage).toEqual('en');
    });

    it('keeps the browser context shared by page views and events', () => {
        // Guards the page/event extraction: both call sites depend on these fields being present.
        expect(buildEventContext()).toMatchObject({
            userAgent: navigator.userAgent,
            language: navigator.language,
            origin: window.location.origin,
        });
    });
});

describe('getMergedTrackingOptions', () => {
    it('should update or create an options object with mixpanel set to the value of what is in localStorage', () => {
        // before anything is set in localStorage
        let trackingOptions = getMergedTrackingOptions();
        expect(trackingOptions).toMatchObject({
            plugins: {
                mixpanel: false,
            },
        });

        localStorage.setItem(THIRD_PARTY_LOGGING_KEY, 'false');
        trackingOptions = getMergedTrackingOptions();
        expect(trackingOptions).toMatchObject({
            plugins: {
                mixpanel: false,
            },
        });

        localStorage.setItem(THIRD_PARTY_LOGGING_KEY, 'true');
        trackingOptions = getMergedTrackingOptions();
        expect(trackingOptions).toMatchObject({
            plugins: {
                mixpanel: true,
            },
        });
    });
});
