import { ListPostsDocument, ListPostsQuery, ListPostsQueryVariables } from '@graphql/post.generated';

type ListPostsPost = NonNullable<ListPostsQuery['listPosts']>['posts'][number];

/**
 * Apply a change to the cached list posts result for the given variables. The variables must match the
 * active listPosts query exactly, otherwise the write lands on a different cache entry and the table won't update.
 */
const modifyListPostCache = (
    client,
    variables: ListPostsQueryVariables,
    modify: (posts: ListPostsPost[]) => ListPostsPost[],
) => {
    const currData: ListPostsQuery | null = client.readQuery({ query: ListPostsDocument, variables });
    if (!currData?.listPosts) return;

    const currPosts = currData.listPosts.posts || [];
    const newPosts = modify(currPosts);
    const delta = newPosts.length - currPosts.length;

    client.writeQuery({
        query: ListPostsDocument,
        variables,
        data: {
            listPosts: {
                ...currData.listPosts,
                count: currData.listPosts.count + delta,
                total: currData.listPosts.total + delta,
                posts: newPosts,
            },
        },
    });
};

/**
 * Update the title and description of an entry in the list posts cache.
 */
export const updateListPostCache = (
    client,
    variables: ListPostsQueryVariables,
    urn: string,
    title: string,
    description: string | null,
) => {
    modifyListPostCache(client, variables, (posts) =>
        posts.map((post) => (post.urn === urn ? { ...post, content: { ...post.content, title, description } } : post)),
    );
};

/**
 * Remove an entry from the list posts cache.
 */
export const removeFromListPostCache = (client, variables: ListPostsQueryVariables, urn: string) => {
    modifyListPostCache(client, variables, (posts) => posts.filter((post) => post.urn !== urn));
};
