/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const LAST_VIEWED_ANNOUNCEMENT_TIME_STEP = 'lastViewedAnnouncementTimeMs';

export const hasViewedAnnouncement = (
    lastViewedAnnouncementsTime: number | null,
    lastModifiedTime?: number | null,
): boolean => {
    return lastModifiedTime && lastViewedAnnouncementsTime ? lastModifiedTime <= lastViewedAnnouncementsTime : true;
};
