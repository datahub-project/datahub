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
