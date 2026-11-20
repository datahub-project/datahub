import { ApolloClient, isReference } from '@apollo/client';

import { ListPostsQuery } from '@graphql/post.generated';
import { UpdatePostContentInput } from '@types';

type ListPostsResult = NonNullable<ListPostsQuery['listPosts']>;
type ListPostsPost = ListPostsResult['posts'][number];

/**
 * Apply a change to every cached listPosts result, so other pages and searches don't show stale posts.
 */
const modifyListPostsCache = (client: ApolloClient<object>, modify: (posts: ListPostsPost[]) => ListPostsPost[]) => {
    client.cache.modify<{ listPosts: ListPostsResult }>({
        id: 'ROOT_QUERY',
        fields: {
            listPosts(existing) {
                if (isReference(existing) || !existing?.posts) return existing;
                const posts = modify(existing.posts);
                const delta = posts.length - existing.posts.length;
                return { ...existing, count: existing.count + delta, total: existing.total + delta, posts };
            },
        },
    });
};

/**
 * Update the content of a post in the list posts cache.
 */
export const updateListPostsCache = (client: ApolloClient<object>, urn: string, content: UpdatePostContentInput) => {
    modifyListPostsCache(client, (posts) =>
        posts.map((post) =>
            post.urn === urn
                ? {
                      ...post,
                      content: {
                          ...post.content,
                          contentType: content.contentType,
                          title: content.title,
                          description: content.description ?? null,
                          link: content.link ?? null,
                          media: content.media ? { __typename: 'Media', ...content.media } : null,
                      },
                  }
                : post,
        ),
    );
};

/**
 * Remove a post from the list posts cache.
 */
export const removeFromListPostsCache = (client: ApolloClient<object>, urn: string) => {
    modifyListPostsCache(client, (posts) => posts.filter((post) => post.urn !== urn));
};

/**
 * Drop all cached listPosts results. Active queries refetch from the server.
 */
export const clearListPostsCache = (client: ApolloClient<object>) => {
    client.cache.evict({ id: 'ROOT_QUERY', fieldName: 'listPosts' });
};
