import { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useGetLastViewedAnnouncementTime } from '@app/homeV2/shared/useGetLastViewedAnnouncementTime';

import { useListPostsQuery } from '@graphql/post.generated';
import { useUpdateUserHomePageSettingsMutation } from '@graphql/user.generated';
import { FilterOperator, Post, PostContentType, PostType } from '@types';

export const useGetAnnouncementsForUser = () => {
    const { user } = useUserContext();
    const { time: lastViewedAnnouncementsTime, loading: lastViewedTimeLoading } = useGetLastViewedAnnouncementTime();
    const [updateUserHomePageSettings] = useUpdateUserHomePageSettingsMutation();
    const [newDismissedUrns, setNewDismissedUrns] = useState<string[]>([]);

    const dismissedUrns = (user?.settings?.homePage?.dismissedAnnouncementUrns || []).filter((urn): urn is string =>
        Boolean(urn),
    );

    const getUserPostsFilters = () => [
        {
            and: [
                {
                    field: 'type',
                    condition: FilterOperator.Equal,
                    values: ['HOME_PAGE_ANNOUNCEMENT'],
                },
                {
                    field: 'urn',
                    condition: FilterOperator.Equal,
                    values: dismissedUrns,
                    negated: true,
                },
                {
                    field: 'lastModified',
                    condition: FilterOperator.GreaterThan,
                    values: [(lastViewedAnnouncementsTime || 0).toString()],
                },
            ],
        },
    ];

    const inputs = {
        start: 0,
        count: 30,
        orFilters: getUserPostsFilters(),
    };

    const {
        data: postsData,
        loading,
        error,
        refetch,
    } = useListPostsQuery({
        variables: {
            input: inputs,
        },
        skip: !user || lastViewedTimeLoading,
    });

    const announcementsData: Post[] =
        postsData?.listPosts?.posts
            .filter((post) => post.postType === PostType.HomePageAnnouncement)
            .filter((post) => post.content.contentType === PostContentType.Text)
            .map((post) => post as Post) || [];

    const onDismissAnnouncement = (urn: string) => {
        setNewDismissedUrns((prev) => [...prev, urn]);

        updateUserHomePageSettings({
            variables: {
                input: {
                    newDismissedAnnouncements: [urn],
                },
            },
        });
    };

    const announcements = announcementsData.filter((announcement) => !newDismissedUrns.includes(announcement.urn));

    return { announcements, loading, error, refetch, onDismissAnnouncement };
};
