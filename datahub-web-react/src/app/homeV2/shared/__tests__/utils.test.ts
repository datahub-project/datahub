import { hasViewedAnnouncement } from '../utils';

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
