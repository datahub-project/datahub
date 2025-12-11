/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useGetLastViewedAnnouncementTime } from '@app/homeV2/shared/useGetLastViewedAnnouncementTime';
import { hasViewedAnnouncement } from '@app/homeV2/shared/utils';
import { getHomePagePostsFilters } from '@app/utils/queryUtils';

import { useListPostsQuery } from '@graphql/post.generated';
import { Post, PostContentType, PostType } from '@types';

export const useGetUnseenAnnouncements = () => {
    const { time: lastViewedAnnouncementsTime } = useGetLastViewedAnnouncementTime();
    const {
        data: postsData,
        loading,
        error,
    } = useListPostsQuery({
        variables: {
            input: {
                start: 0,
                count: 30,
                orFilters: getHomePagePostsFilters(),
            },
        },
        fetchPolicy: 'cache-first',
    });

    const announcements: Post[] =
        postsData?.listPosts?.posts
            .filter((post) => post.postType === PostType.HomePageAnnouncement)
            .filter((post) => post.content.contentType === PostContentType.Text)
            .filter((post) => !hasViewedAnnouncement(lastViewedAnnouncementsTime, post.lastModified?.time))
            .map((post) => post as Post) || [];

    return { announcements, loading, error };
};
