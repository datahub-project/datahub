import { getHomePagePostsFilters } from '@app/utils/queryUtils';
import { useListPostsQuery } from '../../../../../graphql/post.generated';
import { PostContent, PostContentType, PostType } from '../../../../../types.generated';

export const useGetPinnedLinks = () => {
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

    const links: PostContent[] =
        postsData?.listPosts?.posts
            .filter((post) => post.postType === PostType.HomePageAnnouncement)
            .filter((post) => post.content.contentType === PostContentType.Link)
            .map((post) => post.content as PostContent) || [];

    return { links, loading, error };
};
