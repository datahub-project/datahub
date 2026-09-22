import { describe, expect, it } from 'vitest';

import {
    HOME_V2_RECOMMENDATION_MODULE_IDS,
    collectHomeRecommendationModuleIds,
    pageModuleTypeToRecommendationModuleId,
    sortRecommendationModuleIds,
} from '@app/homeV2/homeRecommendationModules';
import { DEFAULT_TEMPLATE } from '@app/homeV3/modules/constants';

import { PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, RecommendationModuleId } from '@types';

function createModule(type: DataHubPageModuleType, urn = `urn:li:dataHubPageModule:${type}`) {
    return {
        urn,
        type: EntityType.DatahubPageModule,
        exists: true,
        properties: {
            name: type,
            type,
            visibility: { scope: PageModuleScope.Global },
            params: {},
        },
    };
}

function createTemplate(moduleTypes: DataHubPageModuleType[]): PageTemplateFragment {
    return {
        urn: 'urn:li:dataHubPageTemplate:test',
        type: EntityType.DatahubPageTemplate,
        properties: {
            rows: [
                {
                    modules: moduleTypes.map((type) => createModule(type)),
                },
            ],
        },
    } as PageTemplateFragment;
}

describe('pageModuleTypeToRecommendationModuleId', () => {
    it('maps Domains and Platforms only', () => {
        expect(pageModuleTypeToRecommendationModuleId(DataHubPageModuleType.Domains)).toBe(
            RecommendationModuleId.Domains,
        );
        expect(pageModuleTypeToRecommendationModuleId(DataHubPageModuleType.Platforms)).toBe(
            RecommendationModuleId.Platforms,
        );
        expect(pageModuleTypeToRecommendationModuleId(DataHubPageModuleType.OwnedAssets)).toBeUndefined();
        expect(pageModuleTypeToRecommendationModuleId(DataHubPageModuleType.Link)).toBeUndefined();
        expect(pageModuleTypeToRecommendationModuleId(undefined)).toBeUndefined();
    });
});

describe('sortRecommendationModuleIds', () => {
    it('dedupes and sorts for a stable Apollo cache key', () => {
        expect(
            sortRecommendationModuleIds([
                RecommendationModuleId.Platforms,
                RecommendationModuleId.Domains,
                RecommendationModuleId.Platforms,
            ]),
        ).toEqual([RecommendationModuleId.Domains, RecommendationModuleId.Platforms]);
    });
});

describe('collectHomeRecommendationModuleIds', () => {
    it('returns only Domains for the default Home V3 template', () => {
        expect(collectHomeRecommendationModuleIds(DEFAULT_TEMPLATE)).toEqual([RecommendationModuleId.Domains]);
    });

    it('includes Platforms when present, sorted independently of template order', () => {
        expect(
            collectHomeRecommendationModuleIds(
                createTemplate([DataHubPageModuleType.Platforms, DataHubPageModuleType.Domains]),
            ),
        ).toEqual([RecommendationModuleId.Domains, RecommendationModuleId.Platforms]);
    });

    it('ignores modules that do not use listRecommendations', () => {
        expect(
            collectHomeRecommendationModuleIds(
                createTemplate([
                    DataHubPageModuleType.OwnedAssets,
                    DataHubPageModuleType.Link,
                    DataHubPageModuleType.RichText,
                ]),
            ),
        ).toEqual([]);
    });

    it('returns an empty list for a missing template so the query can skip', () => {
        expect(collectHomeRecommendationModuleIds(null)).toEqual([]);
        expect(collectHomeRecommendationModuleIds(undefined)).toEqual([]);
    });
});

describe('HOME_V2_RECOMMENDATION_MODULE_IDS', () => {
    it('requests Domains, Platforms, and Top Terms in sorted order', () => {
        expect(HOME_V2_RECOMMENDATION_MODULE_IDS).toEqual([
            RecommendationModuleId.Domains,
            RecommendationModuleId.Platforms,
            RecommendationModuleId.TopTerms,
        ]);
    });
});
