import React from 'react';
import styled from 'styled-components';

import { AnnouncementCard } from '@app/homeV2/content/tabs/announcements/AnnouncementCard';
import AnnouncementsSkeleton from '@app/homeV2/content/tabs/announcements/AnnouncementsSkeleton';
import { EmptyAnnouncements } from '@app/homeV2/content/tabs/announcements/EmptyAnnouncements';
import { useGetAnnouncements } from '@app/homeV2/content/tabs/announcements/useGetAnnouncements';
import { V2_HOME_PAGE_ANNOUNCEMENTS_ID } from '@app/onboarding/configV2/HomePageOnboardingConfig';

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
