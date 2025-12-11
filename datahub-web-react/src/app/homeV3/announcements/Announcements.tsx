/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useCallback } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import ExpandableAnnouncements from '@app/homeV3/announcements/ExpandableAnnouncements';
import { useGetAnnouncementsForUser } from '@app/homeV3/announcements/useGetAnnouncementsForUser';

const AnnouncementsContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding: 0 42px;
    gap: 8px;
`;

export const Announcements = () => {
    const { announcements, onDismissAnnouncement } = useGetAnnouncementsForUser();

    const sortedAnnouncements = announcements.sort((a, b) => {
        return b?.lastModified?.time - a?.lastModified?.time;
    });

    const onDismiss = useCallback(
        (urn: string) => {
            onDismissAnnouncement(urn);

            analytics.event({
                type: EventType.HomePageTemplateModuleAnnouncementDismiss,
            });
        },
        [onDismissAnnouncement],
    );

    return (
        <AnnouncementsContainer>
            <ExpandableAnnouncements announcements={sortedAnnouncements} onDismiss={onDismiss} />
        </AnnouncementsContainer>
    );
};
