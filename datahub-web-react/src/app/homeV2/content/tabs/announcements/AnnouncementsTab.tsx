import React from 'react';
import styled from 'styled-components';
import { useGetAnnouncements } from './useGetAnnouncements';
import { AnnouncementsLoadingSection } from './AnnouncementsLoadingSection';
import { AnnouncementCard } from './AnnouncementCard';

const List = styled.div`
    display: flex;
    flex-direction: column;
    padding: 0px 0px 0px 0px;
    gap: 16px;
`;

export const AnnouncementsTab = () => {
    const { announcements, loading } = useGetAnnouncements();
    const sortedAnnouncements = announcements.sort((a, b) => {
        return b?.lastModified?.time - a?.lastModified?.time;
    });

    return (
        <>
            {loading && <AnnouncementsLoadingSection />}
            <List>
                {sortedAnnouncements.map((announcement) => (
                    <AnnouncementCard
                        key={`${announcement.content.title}-${announcement.content.description}`}
                        announcement={announcement}
                    />
                ))}
            </List>
        </>
    );
};
