/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { hasViewedAnnouncement } from '@app/homeV2/shared/utils';

describe('home V2 utils test ->', () => {
    it('should return whether user has viewed announcement for valid input - true', () => {
        const lastViewedAnnouncementsTime = 1714037282159; // Greater
        const lastModifiedTime = 1714037281109;
        const result = hasViewedAnnouncement(lastViewedAnnouncementsTime, lastModifiedTime);
        expect(result).toBe(true);
    });
    it('should return whether user has viewed announcement for invalid input - false', () => {
        const lastViewedAnnouncementsTime = 1714037281109;
        const lastModifiedTime = 1714037282159; // Greater
        const result = hasViewedAnnouncement(lastViewedAnnouncementsTime, lastModifiedTime);
        expect(result).toBe(false);
    });
});
