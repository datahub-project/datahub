import {
    ensureSelectedAuthorOptions,
    ensureSelectedOptions,
    filterOutAiAgentAuthors,
    isAiAgentUrn,
    isDataPlatformEntity,
    mapFacetToAuthorOptions,
    mapFacetToEntityOptions,
    mapFacetToTypeOptions,
    resolveCreatorFromEntity,
} from '@app/document/utils/documentSidebarFacets.utils';

import { EntityType, FacetMetadata } from '@types';

describe('documentSidebarFacets.utils', () => {
    describe('isAiAgentUrn / filterOutAiAgentAuthors', () => {
        it('detects and drops aiAgent URNs', () => {
            expect(isAiAgentUrn('urn:li:aiAgent:bot')).toBe(true);
            expect(isAiAgentUrn('urn:li:corpuser:jane')).toBe(false);
            expect(
                filterOutAiAgentAuthors([{ value: 'urn:li:corpuser:jane' }, { value: 'urn:li:aiAgent:writer' }]).map(
                    (o) => o.value,
                ),
            ).toEqual(['urn:li:corpuser:jane']);
        });
    });

    describe('isDataPlatformEntity', () => {
        it('narrows DataPlatform entities', () => {
            expect(isDataPlatformEntity({ urn: 'urn:li:dataPlatform:notion', type: EntityType.DataPlatform })).toBe(
                true,
            );
            expect(isDataPlatformEntity({ urn: 'urn:li:corpuser:jane', type: EntityType.CorpUser })).toBe(false);
            expect(isDataPlatformEntity(null)).toBe(false);
        });
    });

    describe('resolveCreatorFromEntity', () => {
        it('maps users and groups; skips other types', () => {
            expect(
                resolveCreatorFromEntity(
                    {
                        urn: 'urn:li:corpuser:jane',
                        type: EntityType.CorpUser,
                        editableProperties: { pictureLink: 'http://img' },
                    } as any,
                    'Jane',
                ),
            ).toEqual({
                urn: 'urn:li:corpuser:jane',
                type: EntityType.CorpUser,
                displayName: 'Jane',
                pictureLink: 'http://img',
            });
            expect(
                resolveCreatorFromEntity({ urn: 'urn:li:corpGroup:eng', type: EntityType.CorpGroup } as any, 'Eng'),
            ).toEqual({
                urn: 'urn:li:corpGroup:eng',
                type: EntityType.CorpGroup,
                displayName: 'Eng',
                pictureLink: null,
            });
            expect(
                resolveCreatorFromEntity(
                    { urn: 'urn:li:dataPlatform:notion', type: EntityType.DataPlatform } as any,
                    'Notion',
                ),
            ).toBeNull();
        });
    });

    describe('mapFacetTo*Options', () => {
        const getDisplayName = (_type: EntityType, entity: { urn: string }) => `name:${entity.urn}`;

        it('maps type / entity / author facets and drops zero counts', () => {
            const typesFacet = {
                field: 'typeNames',
                aggregations: [
                    { value: 'runbook', count: 2 },
                    { value: 'faq', count: 0 },
                ],
            } as FacetMetadata;

            expect(mapFacetToTypeOptions(typesFacet)).toEqual([{ value: 'runbook', label: 'Runbook' }]);

            const domainFacet = {
                field: 'domains',
                aggregations: [
                    {
                        value: 'urn:li:domain:eng',
                        count: 1,
                        entity: { urn: 'urn:li:domain:eng', type: EntityType.Domain },
                    },
                ],
            } as FacetMetadata;

            expect(mapFacetToEntityOptions(domainFacet, getDisplayName as any)).toEqual([
                {
                    value: 'urn:li:domain:eng',
                    label: 'name:urn:li:domain:eng',
                    entity: { urn: 'urn:li:domain:eng', type: EntityType.Domain },
                },
            ]);

            const creatorsFacet = {
                field: 'creator',
                aggregations: [
                    {
                        value: 'urn:li:corpuser:jane',
                        count: 3,
                        entity: { urn: 'urn:li:corpuser:jane', type: EntityType.CorpUser },
                    },
                    { value: 'urn:li:corpuser:ghost', count: 1, entity: null },
                ],
            } as FacetMetadata;

            expect(mapFacetToAuthorOptions(creatorsFacet, getDisplayName as any)).toEqual([
                {
                    value: 'urn:li:corpuser:jane',
                    label: 'name:urn:li:corpuser:jane',
                    entity: { urn: 'urn:li:corpuser:jane', type: EntityType.CorpUser },
                    creator: {
                        urn: 'urn:li:corpuser:jane',
                        type: EntityType.CorpUser,
                        displayName: 'name:urn:li:corpuser:jane',
                        pictureLink: null,
                    },
                },
            ]);
        });
    });

    describe('ensureSelected*', () => {
        it('preserves selected values missing from aggregations', () => {
            expect(ensureSelectedOptions([{ value: 'a', label: 'A' }], ['a', 'b'])).toEqual([
                { value: 'a', label: 'A' },
                { value: 'b', label: 'b' },
            ]);
            expect(ensureSelectedAuthorOptions([], ['urn:li:corpuser:jane'])[0].creator.urn).toBe(
                'urn:li:corpuser:jane',
            );
        });
    });
});
