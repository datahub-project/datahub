import React from 'react';
import styled from 'styled-components';

import { AnnouncementCard } from '@app/homeV3/announcements/AnnouncementCard';
import { useGetAnnouncementsForUser } from '@app/homeV3/announcements/useGetAnnouncementsForUser';

const AnnouncementsContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding: 0;
    gap: 8px;
`;

export const Announcements = () => {
    const { announcements, onDismissAnnouncement } = useGetAnnouncementsForUser();

    const sortedAnnouncements = announcements.sort((a, b) => {
        return b?.lastModified?.time - a?.lastModified?.time;
    });

    return (
        <AnnouncementsContainer>
            {sortedAnnouncements.map((announcement) => (
                <AnnouncementCard
                    key={announcement.urn}
                    announcement={announcement}
                    onDismiss={onDismissAnnouncement}
                />
            ))}
        </AnnouncementsContainer>
    );
};
