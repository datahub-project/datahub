import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { HOME_V2_RECOMMENDATION_MODULE_IDS } from '@app/homeV2/homeRecommendationModules';
import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useShowHomePageRedesign } from '@app/homeV3/context/hooks/useShowHomePageRedesign';
import { DEFAULT_TEMPLATE } from '@app/homeV3/modules/constants';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, RecommendationModuleId, ScenarioType } from '@types';

vi.mock('@graphql/recommendations.generated', () => ({
    useListRecommendationsQuery: vi.fn(),
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/homeV3/context/hooks/useShowHomePageRedesign', () => ({
    useShowHomePageRedesign: vi.fn(),
}));

vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: vi.fn(),
}));

describe('useHomeRecommendations', () => {
    const queryMock = useListRecommendationsQuery as unknown as Mock;
    const userMock = useUserContext as unknown as Mock;
    const showV3Mock = useShowHomePageRedesign as unknown as Mock;
    const templateMock = usePageTemplateContext as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        queryMock.mockReturnValue({
            data: undefined,
            loading: false,
            refetch: vi.fn(),
        });
        userMock.mockReturnValue({
            user: { urn: 'urn:li:corpuser:alice' },
            localState: { selectedViewUrn: 'urn:li:view:1' },
        });
    });

    it('skips until the Home V3 template is known', () => {
        showV3Mock.mockReturnValue(true);
        templateMock.mockReturnValue({ template: null });

        renderHook(() => useHomeRecommendations());

        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        requestContext: {
                            scenario: ScenarioType.Home,
                            modules: [],
                        },
                    }),
                }),
            }),
        );
    });

    it('requests only Domains for the default Home V3 template', () => {
        showV3Mock.mockReturnValue(true);
        templateMock.mockReturnValue({ template: DEFAULT_TEMPLATE });

        renderHook(() => useHomeRecommendations());

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    userUrn: 'urn:li:corpuser:alice',
                    requestContext: {
                        scenario: ScenarioType.Home,
                        modules: [RecommendationModuleId.Domains],
                    },
                    limit: 1,
                    viewUrn: 'urn:li:view:1',
                },
            },
            fetchPolicy: 'cache-first',
            skip: false,
        });
    });

    it('adds Platforms when the template includes it, in stable order', () => {
        showV3Mock.mockReturnValue(true);
        templateMock.mockReturnValue({
            template: {
                urn: 'urn:li:dataHubPageTemplate:custom',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [
                        {
                            modules: [
                                {
                                    urn: 'urn:li:dataHubPageModule:platforms',
                                    type: EntityType.DatahubPageModule,
                                    exists: true,
                                    properties: {
                                        name: 'Platforms',
                                        type: DataHubPageModuleType.Platforms,
                                        visibility: { scope: PageModuleScope.Global },
                                        params: {},
                                    },
                                },
                                {
                                    urn: 'urn:li:dataHubPageModule:top_domains',
                                    type: EntityType.DatahubPageModule,
                                    exists: true,
                                    properties: {
                                        name: 'Domains',
                                        type: DataHubPageModuleType.Domains,
                                        visibility: { scope: PageModuleScope.Global },
                                        params: {},
                                    },
                                },
                            ],
                        },
                    ],
                },
            },
        });

        renderHook(() => useHomeRecommendations());

        expect(queryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: false,
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        requestContext: {
                            scenario: ScenarioType.Home,
                            modules: [RecommendationModuleId.Domains, RecommendationModuleId.Platforms],
                        },
                        limit: 2,
                    }),
                }),
            }),
        );
    });

    it('skips when the Home V3 template has no recommendation modules', () => {
        showV3Mock.mockReturnValue(true);
        templateMock.mockReturnValue({
            template: {
                urn: 'urn:li:dataHubPageTemplate:assets-only',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [
                        {
                            modules: [
                                {
                                    urn: 'urn:li:dataHubPageModule:your_assets',
                                    type: EntityType.DatahubPageModule,
                                    exists: true,
                                    properties: {
                                        name: 'Your Assets',
                                        type: DataHubPageModuleType.OwnedAssets,
                                        visibility: { scope: PageModuleScope.Global },
                                        params: {},
                                    },
                                },
                            ],
                        },
                    ],
                },
            },
        });

        renderHook(() => useHomeRecommendations());

        expect(queryMock).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
    });

    it('uses the Home V2 module set when redesign is off', () => {
        showV3Mock.mockReturnValue(false);
        templateMock.mockReturnValue({ template: null });

        renderHook(() => useHomeRecommendations());

        expect(queryMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    userUrn: 'urn:li:corpuser:alice',
                    requestContext: {
                        scenario: ScenarioType.Home,
                        modules: HOME_V2_RECOMMENDATION_MODULE_IDS,
                    },
                    limit: HOME_V2_RECOMMENDATION_MODULE_IDS.length,
                    viewUrn: 'urn:li:view:1',
                },
            },
            fetchPolicy: 'cache-first',
            skip: false,
        });
    });

    it('skips when the user urn is missing', () => {
        showV3Mock.mockReturnValue(false);
        templateMock.mockReturnValue({ template: null });
        userMock.mockReturnValue({
            user: undefined,
            localState: { selectedViewUrn: undefined },
        });

        renderHook(() => useHomeRecommendations());

        expect(queryMock).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
    });
});
