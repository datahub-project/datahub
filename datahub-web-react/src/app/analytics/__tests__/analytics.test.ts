import { getMergedTrackingOptions, THIRD_PARTY_LOGGING_KEY } from '../analytics';

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
