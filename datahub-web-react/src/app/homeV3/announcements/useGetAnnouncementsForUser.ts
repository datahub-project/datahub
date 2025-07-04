import { useUserContext } from '@app/context/useUserContext';
import { useGetLastViewedAnnouncementTime } from '@app/homeV2/shared/useGetLastViewedAnnouncementTime';

import { useListPostsQuery } from '@graphql/post.generated';
import { useUpdateUserHomePageSettingsMutation } from '@graphql/user.generated';
import { FilterOperator, Post, PostContentType, PostType } from '@types';

export const useGetAnnouncementsForUser = () => {
    const { user } = useUserContext();
    const { time: lastViewedAnnouncementsTime } = useGetLastViewedAnnouncementTime();
    const [updateUserHomePageSettings] = useUpdateUserHomePageSettingsMutation();

    const dismissedUrns = (user?.settings?.homePage?.dismissedAnnouncements || []).filter((urn): urn is string =>
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
        fetchPolicy: 'cache-first',
        skip: !user,
    });

    const announcements: Post[] =
        postsData?.listPosts?.posts
            .filter((post) => post.postType === PostType.HomePageAnnouncement)
            .filter((post) => post.content.contentType === PostContentType.Text)
            .map((post) => post as Post) || [];

    const onDismissAnnouncement = (urn: string) => {
        const updatedDismissedUrns = [...dismissedUrns, urn];

        const updatedFilters = getUserPostsFilters().map((filter) => ({
            ...filter,
            and: filter.and.map((facet) =>
                facet.field === 'urn' ? { ...facet, values: updatedDismissedUrns } : facet,
            ),
        }));
        updateUserHomePageSettings({
            variables: {
                input: {
                    newDismissedAnnouncements: [urn],
                },
            },
        }).then(() => {
            setTimeout(() => {
                refetch({ input: { ...inputs, orFilters: updatedFilters } });
            }, 2000);
        });
    };

    return { announcements, loading, error, refetch, onDismissAnnouncement };
};
