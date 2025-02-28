import React from 'react';
import styled from 'styled-components';
import { useGetAnnouncements } from './useGetAnnouncements';
import AnnouncementsSkeleton from './AnnouncementsSkeleton';
import { AnnouncementCard } from './AnnouncementCard';
import { V2_HOME_PAGE_ANNOUNCEMENTS_ID } from '../../../../onboarding/configV2/HomePageOnboardingConfig';
import { EmptyAnnouncements } from './EmptyAnnouncements';

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
            {loading && <AnnouncementsSkeleton />}
            <List id={V2_HOME_PAGE_ANNOUNCEMENTS_ID}>
                {sortedAnnouncements?.length ? (
                    sortedAnnouncements.map((announcement) => (
                        <AnnouncementCard
                            key={`${announcement.content.title}-${announcement.content.description}`}
                            announcement={announcement}
                        />
                    ))
                ) : (
                    <EmptyAnnouncements />
                )}
            </List>
        </>
    );
};
