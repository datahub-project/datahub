import { ListPostsDocument, ListPostsQuery } from '../../../graphql/post.generated';

/**
 * Add an entry to the list posts cache.
 */
export const addToListPostCache = (client, newPost, pageSize) => {
    // Read the data from our cache for this query.
    const currData: ListPostsQuery | null = client.readQuery({
        query: ListPostsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
    });

    // Add our new post into the existing list.
    const newPosts = [...(currData?.listPosts?.posts || [])];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListPostsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
        data: {
            listPosts: {
                start: 0,
                count: (currData?.listPosts?.count || 0) + 1,
                total: (currData?.listPosts?.total || 0) + 1,
                posts: newPosts,
            },
        },
    });
};

/**
 * Remove an entry from the list posts cache.
 */
export const removeFromListPostCache = (client, urn, page, pageSize) => {
    // Read the data from our cache for this query.
    const currData: ListPostsQuery | null = client.readQuery({
        query: ListPostsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
    });

    // Remove the post from the existing posts set.
    const newPosts = [...(currData?.listPosts?.posts || []).filter((post) => post.urn !== urn)];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListPostsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
        data: {
            listPosts: {
                start: currData?.listPosts?.start || 0,
                count: (currData?.listPosts?.count || 1) - 1,
                total: (currData?.listPosts?.total || 1) - 1,
                posts: newPosts,
            },
        },
    });
};
