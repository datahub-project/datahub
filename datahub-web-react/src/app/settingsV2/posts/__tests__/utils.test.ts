import { ApolloClient, InMemoryCache } from '@apollo/client';

import { clearListPostsCache, removeFromListPostsCache, updateListPostsCache } from '@app/settingsV2/posts/utils';

import { ListPostsDocument, ListPostsQuery } from '@graphql/post.generated';
import { EntityType, MediaType, PostContentType, PostType } from '@types';

const PAGE_1 = { input: { start: 0, count: 2 } };
const SEARCH = { input: { start: 0, count: 2, query: 'release' } };

function makePost(urn: string, title: string) {
    return {
        __typename: 'Post' as const,
        urn,
        type: EntityType.Post,
        postType: PostType.HomePageAnnouncement,
        lastModified: { __typename: 'AuditStamp' as const, time: 1 },
        content: {
            __typename: 'PostContent' as const,
            contentType: PostContentType.Text,
            title,
            description: null,
            link: null,
            media: null,
        },
    };
}

function writeList(client: ApolloClient<object>, variables: object, urns: string[]) {
    client.writeQuery({
        query: ListPostsDocument,
        variables,
        data: {
            listPosts: {
                __typename: 'ListPostsResult',
                start: 0,
                count: urns.length,
                total: 5,
                posts: urns.map((urn) => makePost(urn, `title ${urn}`)),
            },
        },
    });
}

function readList(client: ApolloClient<object>, variables: object) {
    return client.readQuery<ListPostsQuery>({ query: ListPostsDocument, variables })?.listPosts;
}

describe('settingsV2 posts cache utils', () => {
    let client: ApolloClient<object>;

    beforeEach(() => {
        client = new ApolloClient({ cache: new InMemoryCache() });
        writeList(client, PAGE_1, ['urn:li:post:a', 'urn:li:post:b']);
        writeList(client, SEARCH, ['urn:li:post:b']);
    });

    it('removes a post from every cached list and adjusts the counts', () => {
        removeFromListPostsCache(client, 'urn:li:post:b');

        const page = readList(client, PAGE_1);
        expect(page?.posts.map((post) => post.urn)).toEqual(['urn:li:post:a']);
        expect(page?.count).toBe(1);
        expect(page?.total).toBe(4);
        expect(readList(client, SEARCH)?.posts).toEqual([]);
    });

    it('updates the content of a post in every cached list', () => {
        updateListPostsCache(client, 'urn:li:post:b', {
            contentType: PostContentType.Link,
            title: 'Docs',
            description: null,
            link: 'https://example.com',
            media: { type: MediaType.Image, location: 'https://example.com/logo.png' },
        });

        [PAGE_1, SEARCH].forEach((variables) => {
            const updated = readList(client, variables)?.posts.find((post) => post.urn === 'urn:li:post:b');
            expect(updated?.content).toMatchObject({
                contentType: PostContentType.Link,
                title: 'Docs',
                link: 'https://example.com',
                media: { location: 'https://example.com/logo.png' },
            });
        });
        expect(readList(client, PAGE_1)?.posts[0].content.title).toBe('title urn:li:post:a');
    });

    it('clears every cached list', () => {
        clearListPostsCache(client);

        expect(readList(client, PAGE_1)).toBeUndefined();
        expect(readList(client, SEARCH)).toBeUndefined();
    });
});
