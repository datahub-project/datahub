import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { graphqlStubData, hasGraphqlFixture } from './stub-graphql.mjs';

const ACTOR = 'urn:li:corpuser:boot';

function homeRecommendations(requestContext, limit = 10) {
    return graphqlStubData('listRecommendations', {
        input: { userUrn: ACTOR, requestContext, limit },
    }).listRecommendations.modules;
}

function ownedSearch(types, values = [ACTOR]) {
    return graphqlStubData('getSearchResultsForMultipleCards', {
        input: { query: '*', start: 0, count: 50, types, filters: [{ field: 'owners', values }] },
    }).searchAcrossEntities;
}

describe('graphqlStubData', () => {
    it('returns a recorded payload only for own fixture names', () => {
        assert.equal(hasGraphqlFixture('getMe'), true);
        assert.equal(graphqlStubData('getMe').me.corpUser.urn, 'urn:li:corpuser:boot');
        assert.equal(hasGraphqlFixture('toString'), false);
        assert.deepEqual(graphqlStubData('toString'), {});
        assert.deepEqual(graphqlStubData('__proto__'), {});
    });

    it('records every operation the signed-in home page requests', () => {
        for (const name of [
            'batchGetStepStates',
            'getQuickFilters',
            'getSearchResultsForMultipleCards',
            'getUserGroupsUrns',
            'listGlobalViews',
            'listMyViews',
            'listPosts',
            'listRecommendations',
        ]) {
            assert.equal(hasGraphqlFixture(name), true, name);
            assert.notDeepEqual(graphqlStubData(name), {}, name);
        }
    });
});

describe('home page fixtures', () => {
    it('marks every requested onboarding step as seen', () => {
        const ids = [`${ACTOR}-search-results-filters`, `${ACTOR}-lastViewedAnnouncementTimeMs`];
        const { results } = graphqlStubData('batchGetStepStates', { input: { ids } }).batchGetStepStates;
        assert.deepEqual(
            results.map((step) => step.id),
            ids,
        );
        const lastViewed = results[1].properties.find((entry) => entry.key === 'lastViewedAnnouncementTimeMs');
        assert.ok(Number(lastViewed.value) > 0);
    });

    it('answers Your Assets with typed entities the user owns', () => {
        const result = ownedSearch([]);
        assert.ok(result.total >= 5);
        assert.equal(result.searchResults.length, result.total);
        const types = new Set(result.searchResults.map(({ entity }) => entity.__typename));
        assert.ok(types.has('Dataset') && types.has('Dashboard'));
        for (const { entity } of result.searchResults) {
            assert.ok(entity.urn && entity.type && entity.platform.__typename === 'DataPlatform');
        }
    });

    it('pages owned assets by start and count', () => {
        const page = graphqlStubData('getSearchResultsForMultipleCards', {
            input: { start: 2, count: 2, types: [], filters: [{ field: 'owners', values: [ACTOR] }] },
        }).searchAcrossEntities;
        assert.equal(page.start, 2);
        assert.equal(page.searchResults.length, 2);
        assert.equal(page.searchResults[0].entity.urn, ownedSearch([]).searchResults[2].entity.urn);
    });

    it('scopes owned search to the requested type and owner', () => {
        assert.ok(ownedSearch(['DOMAIN']).searchResults.every(({ entity }) => entity.__typename === 'Domain'));
        assert.equal(ownedSearch(['TAG']).total, 0);
        assert.equal(ownedSearch([], ['urn:li:corpuser:someone-else']).total, 0);
    });

    it('returns the modules a HOME request filters for', () => {
        const [domains] = homeRecommendations({ scenario: 'HOME', modules: ['DOMAINS'] });
        assert.equal(domains.moduleId, 'Domains');
        assert.equal(domains.renderType, 'DOMAIN_SEARCH_LIST');
        assert.ok(domains.content.length >= 3);
        for (const content of domains.content) {
            assert.equal(content.entity.__typename, 'Domain');
            assert.ok(content.params.contentParams.count > 0);
        }

        const [recent] = homeRecommendations({ scenario: 'HOME', modules: ['RECENTLY_VIEWED_ENTITIES'] }, 1);
        assert.equal(recent.moduleId, 'RecentlyViewedEntities');
        assert.ok(recent.content.every((content) => content.entity.urn === content.params.entityProfileParams.urn));
    });

    it('caps the module count at the request limit', () => {
        assert.ok(homeRecommendations({ scenario: 'HOME' }).length > 1);
        assert.equal(homeRecommendations({ scenario: 'HOME' }, 1).length, 1);
        assert.equal(homeRecommendations({ scenario: 'SEARCH_BAR' })[0].renderType, 'SEARCH_QUERY_LIST');
    });

    it('reads owners from orFilters as well as filters', () => {
        const result = graphqlStubData('getSearchResultsForMultipleCards', {
            input: { types: [], filters: [], orFilters: [{ and: [{ field: 'owners', values: [ACTOR] }] }] },
        }).searchAcrossEntities;
        assert.equal(result.total, ownedSearch([]).total);
    });

    it('returns only announcements newer than the last visit and not dismissed', () => {
        const all = graphqlStubData('listPosts', { input: { start: 0, count: 30 } }).listPosts.posts;
        const newest = Math.max(...all.map((post) => post.lastModified.time));
        const since = (time, dismissed = []) =>
            graphqlStubData('listPosts', {
                input: {
                    start: 0,
                    count: 30,
                    orFilters: [
                        {
                            and: [
                                { field: 'type', condition: 'EQUAL', values: ['HOME_PAGE_ANNOUNCEMENT'] },
                                { field: 'urn', condition: 'EQUAL', values: dismissed, negated: true },
                                { field: 'lastModified', condition: 'GREATER_THAN', values: [String(time)] },
                            ],
                        },
                    ],
                },
            }).listPosts;
        assert.equal(since(0).total, all.length);
        assert.equal(since(newest).total, 0);
        assert.ok(since(newest - 1).posts.every((post) => post.lastModified.time === newest));
        assert.equal(since(0, [all[0].urn]).total, all.length - 1);
    });

    it('does not throw on malformed variables', () => {
        const odd = [
            null,
            [],
            { input: null },
            { input: { ids: 'x', filters: [null], orFilters: [null, { and: null }] } },
        ];
        for (const variables of odd) {
            for (const name of [
                'batchGetStepStates',
                'getSearchResultsForMultipleCards',
                'listPosts',
                'listRecommendations',
                'getUserGroupsUrns',
                'listGlobalViews',
            ]) {
                assert.doesNotThrow(() => graphqlStubData(name, variables), `${name} ${JSON.stringify(variables)}`);
            }
        }
    });

    it('lists home page announcements, quick filters, groups and views', () => {
        const { posts, total } = graphqlStubData('listPosts', { input: { start: 0, count: 10 } }).listPosts;
        assert.ok(total >= 1 && posts.length === total);
        assert.ok(posts.every((post) => post.postType === 'HOME_PAGE_ANNOUNCEMENT' && post.content.title));

        const { quickFilters } = graphqlStubData('getQuickFilters', { input: {} }).getQuickFilters;
        assert.ok(quickFilters.length > 0 && quickFilters.every((filter) => filter.field === 'platform'));

        const groups = graphqlStubData('getUserGroupsUrns', { urn: ACTOR, start: 0, count: 500 }).corpUser
            .relationships;
        assert.ok(groups.total > 0 && groups.relationships.every(({ entity }) => entity.type === 'CORP_GROUP'));

        const global = graphqlStubData('listGlobalViews', { start: 0, count: 10 }).listGlobalViews;
        const mine = graphqlStubData('listMyViews', { start: 0, count: 10, viewType: 'PERSONAL' }).listMyViews;
        assert.ok(global.views.every((view) => view.viewType === 'GLOBAL'));
        assert.ok(mine.views.every((view) => view.viewType === 'PERSONAL'));
    });
});
