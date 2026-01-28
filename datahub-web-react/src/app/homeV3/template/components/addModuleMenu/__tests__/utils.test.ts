import { getCustomGlobalModules } from '@app/homeV3/template/components/addModuleMenu/utils';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Helper function to create mock modules
const createMockModule = (name: string, urn: string, type: DataHubPageModuleType): PageModuleFragment => ({
    urn,
    type: EntityType.DatahubPageModule,
    properties: {
        name,
        type,
        visibility: { scope: PageModuleScope.Personal },
        params: {},
    },
});

// Helper function to create mock template
const createMockTemplate = (modules: PageModuleFragment[][]): PageTemplateFragment => ({
    urn: 'urn:li:pageTemplate:test',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows: modules.map((rowModules) => ({
            modules: rowModules,
        })),
        surface: { surfaceType: PageTemplateSurfaceType.HomePage },
        visibility: { scope: PageTemplateScope.Global },
    },
});

describe('getCustomGlobalModules', () => {
    describe('null and empty template handling', () => {
        it('should return empty array when template is null', () => {
            const result = getCustomGlobalModules(null);
            expect(result).toEqual([]);
        });

        it('should return empty array when template has no rows', () => {
            const template = createMockTemplate([]);
            const result = getCustomGlobalModules(template);
            expect(result).toEqual([]);
        });

        it('should return empty array when template rows have no modules', () => {
            const template = createMockTemplate([[], []]);
            const result = getCustomGlobalModules(template);
            expect(result).toEqual([]);
        });
    });

    describe('custom module filtering', () => {
        it('should return only custom modules (Link, RichText, Hierarchy, AssetCollection)', () => {
            const customModules = [
                createMockModule('Link Module', 'urn:li:pageModule:link', DataHubPageModuleType.Link),
                createMockModule('RichText Module', 'urn:li:pageModule:richtext', DataHubPageModuleType.RichText),
                createMockModule('Hierarchy Module', 'urn:li:pageModule:hierarchy', DataHubPageModuleType.Hierarchy),
                createMockModule(
                    'AssetCollection Module',
                    'urn:li:pageModule:collection',
                    DataHubPageModuleType.AssetCollection,
                ),
            ];

            const template = createMockTemplate([customModules]);
            const result = getCustomGlobalModules(template);

            expect(result).toHaveLength(4);
            expect(result).toEqual(customModules);
        });

        it('should exclude non-custom modules (OwnedAssets, Domains)', () => {
            const mixedModules = [
                createMockModule('Link Module', 'urn:li:pageModule:link', DataHubPageModuleType.Link),
                createMockModule('OwnedAssets Module', 'urn:li:pageModule:owned', DataHubPageModuleType.OwnedAssets),
                createMockModule('RichText Module', 'urn:li:pageModule:richtext', DataHubPageModuleType.RichText),
                createMockModule('Domains Module', 'urn:li:pageModule:domains', DataHubPageModuleType.Domains),
            ];

            const template = createMockTemplate([mixedModules]);
            const result = getCustomGlobalModules(template);

            expect(result).toHaveLength(2);
            expect(result[0].properties.type).toBe(DataHubPageModuleType.Link);
            expect(result[1].properties.type).toBe(DataHubPageModuleType.RichText);
        });

        it('should return empty array when template contains only non-custom modules', () => {
            const nonCustomModules = [
                createMockModule('OwnedAssets Module', 'urn:li:pageModule:owned', DataHubPageModuleType.OwnedAssets),
                createMockModule('Domains Module', 'urn:li:pageModule:domains', DataHubPageModuleType.Domains),
            ];

            const template = createMockTemplate([nonCustomModules]);
            const result = getCustomGlobalModules(template);

            expect(result).toEqual([]);
        });
    });

    describe('multiple rows handling', () => {
        it('should collect custom modules from multiple rows', () => {
            const row1Modules = [
                createMockModule('Link Module 1', 'urn:li:pageModule:link1', DataHubPageModuleType.Link),
                createMockModule('OwnedAssets Module', 'urn:li:pageModule:owned', DataHubPageModuleType.OwnedAssets),
            ];

            const row2Modules = [
                createMockModule('RichText Module', 'urn:li:pageModule:richtext', DataHubPageModuleType.RichText),
                createMockModule('Domains Module', 'urn:li:pageModule:domains', DataHubPageModuleType.Domains),
            ];

            const row3Modules = [
                createMockModule(
                    'AssetCollection Module',
                    'urn:li:pageModule:collection',
                    DataHubPageModuleType.AssetCollection,
                ),
            ];

            const template = createMockTemplate([row1Modules, row2Modules, row3Modules]);
            const result = getCustomGlobalModules(template);

            expect(result).toHaveLength(3);
            expect(result[0].properties.name).toBe('Link Module 1');
            expect(result[1].properties.name).toBe('RichText Module');
            expect(result[2].properties.name).toBe('AssetCollection Module');
        });

        it('should handle rows with mixed content correctly', () => {
            const mixedRows = [
                [], // Empty row
                [createMockModule('OwnedAssets Module', 'urn:li:pageModule:owned', DataHubPageModuleType.OwnedAssets)], // Non-custom only
                [createMockModule('Link Module', 'urn:li:pageModule:link', DataHubPageModuleType.Link)], // Custom only
                [
                    createMockModule(
                        'Hierarchy Module',
                        'urn:li:pageModule:hierarchy',
                        DataHubPageModuleType.Hierarchy,
                    ),
                    createMockModule('Domains Module', 'urn:li:pageModule:domains', DataHubPageModuleType.Domains),
                ], // Mixed
            ];

            const template = createMockTemplate(mixedRows);
            const result = getCustomGlobalModules(template);

            expect(result).toHaveLength(2);
            expect(result[0].properties.type).toBe(DataHubPageModuleType.Link);
            expect(result[1].properties.type).toBe(DataHubPageModuleType.Hierarchy);
        });
    });

    describe('edge cases', () => {
        it('should preserve module order when collecting from multiple rows', () => {
            const row1 = [
                createMockModule('Link Module A', 'urn:li:pageModule:linkA', DataHubPageModuleType.Link),
                createMockModule('RichText Module B', 'urn:li:pageModule:richtextB', DataHubPageModuleType.RichText),
            ];

            const row2 = [
                createMockModule('Hierarchy Module C', 'urn:li:pageModule:hierarchyC', DataHubPageModuleType.Hierarchy),
                createMockModule(
                    'AssetCollection Module D',
                    'urn:li:pageModule:collectionD',
                    DataHubPageModuleType.AssetCollection,
                ),
            ];

            const template = createMockTemplate([row1, row2]);
            const result = getCustomGlobalModules(template);

            expect(result).toHaveLength(4);
            expect(result[0].properties.name).toBe('Link Module A');
            expect(result[1].properties.name).toBe('RichText Module B');
            expect(result[2].properties.name).toBe('Hierarchy Module C');
            expect(result[3].properties.name).toBe('AssetCollection Module D');
        });

        it('should handle single row with single custom module', () => {
            const singleModule = [
                createMockModule('Single Link Module', 'urn:li:pageModule:single', DataHubPageModuleType.Link),
            ];

            const template = createMockTemplate([singleModule]);
            const result = getCustomGlobalModules(template);

            expect(result).toHaveLength(1);
            expect(result[0].properties.name).toBe('Single Link Module');
        });

        it('should return array with distinct modules (no duplicates expected from structure)', () => {
            const modules = [
                createMockModule('Link Module 1', 'urn:li:pageModule:link1', DataHubPageModuleType.Link),
                createMockModule('Link Module 2', 'urn:li:pageModule:link2', DataHubPageModuleType.Link),
                createMockModule('RichText Module 1', 'urn:li:pageModule:richtext1', DataHubPageModuleType.RichText),
            ];

            const template = createMockTemplate([modules]);
            const result = getCustomGlobalModules(template);

            expect(result).toHaveLength(3);

            // Verify each module is distinct
            const urns = result.map((module) => module.urn);
            expect(new Set(urns).size).toBe(urns.length);
        });
    });
});
