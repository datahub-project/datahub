/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { THIRD_PARTY_LOGGING_KEY, getMergedTrackingOptions } from '@app/analytics/analytics';

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
