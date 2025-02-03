import { getHomePagePostsFilters } from '@app/utils/queryUtils';
import { useListPostsQuery } from '../../../../../graphql/post.generated';
import { Post, PostContentType, PostType } from '../../../../../types.generated';

export const useGetAnnouncements = () => {
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
            .map((post) => post as Post) || [];

    return { announcements, loading, error };
};
