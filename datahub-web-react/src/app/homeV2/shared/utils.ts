export const LAST_VIEWED_ANNOUNCEMENT_TIME_STEP = 'lastViewedAnnouncementTimeMs';

export const hasViewedAnnouncement = (
    lastViewedAnnouncementsTime: number | null,
    lastModifiedTime?: number | null,
): boolean => {
    return lastModifiedTime && lastViewedAnnouncementsTime ? lastModifiedTime <= lastViewedAnnouncementsTime : true;
};
