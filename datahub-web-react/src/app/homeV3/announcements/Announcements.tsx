import React from 'react';
import styled from 'styled-components';

import { useGetAnnouncements } from '@app/homeV2/content/tabs/announcements/useGetAnnouncements';
import { AnnouncementCard } from '@app/homeV3/announcements/AnnouncementCard';
import { CenteredContainer } from '@app/homeV3/styledComponents';

const AnnouncementsContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding: 0;
    gap: 8px;
`;

export const Announcements = () => {
    const { announcements } = useGetAnnouncements();
    const sortedAnnouncements = announcements.sort((a, b) => {
        return b?.lastModified?.time - a?.lastModified?.time;
    });

    return (
        <CenteredContainer>
            <AnnouncementsContainer>
                {sortedAnnouncements.map((announcement) => (
                    <AnnouncementCard key={announcement.urn} announcement={announcement} />
                ))}
            </AnnouncementsContainer>
        </CenteredContainer>
    );
};
