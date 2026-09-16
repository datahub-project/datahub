import { describe, expect, it } from 'vitest';

import { getChildHierarchyModule } from '@app/entityV2/summary/modules/childHierarchy/utils';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

describe('getChildHierarchyModule', () => {
    const mockModule: PageModuleFragment = {
        urn: 'urn:li:dataHubPageModule:test1',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Original Name',
            type: DataHubPageModuleType.ChildHierarchy,
            visibility: { scope: PageModuleScope.Global },
            params: {
                hierarchyViewParams: { assetUrns: [], showRelatedEntities: true },
            },
        },
    };

    const testUrn = 'urn:li:domain:test-domain';

    describe('GlossaryNode entity type', () => {
        it('should set name to "Contents" for GlossaryNode', () => {
            const result = getChildHierarchyModule(mockModule, testUrn, EntityType.GlossaryNode);

            expect(result.properties.name).toBe('Contents');
        });

        it('should set showRelatedEntities to true for GlossaryNode', () => {
            const result = getChildHierarchyModule(mockModule, testUrn, EntityType.GlossaryNode);

            expect(result.properties.params.hierarchyViewParams.showRelatedEntities).toBe(true);
        });

        it('should include relatedEntitiesFilterJson for GlossaryNode', () => {
            const result = getChildHierarchyModule(mockModule, testUrn, EntityType.GlossaryNode);

            expect(result.properties.params.hierarchyViewParams.relatedEntitiesFilterJson).toBe(
                '{"type":"logical","operator":"and","operands":[]}',
            );
        });

        it('should preserve existing module properties and params for GlossaryNode', () => {
            const result = getChildHierarchyModule(mockModule, testUrn, EntityType.GlossaryNode);

            expect(result.type).toBe(mockModule.type);
            expect(result.properties.params.hierarchyViewParams).toMatchObject({
                assetUrns: [testUrn],
                relatedEntitiesFilterJson: '{"type":"logical","operator":"and","operands":[]}',
                showRelatedEntities: true,
            });
            expect(result.properties.visibility).toBe(mockModule.properties.visibility);
        });
    });

    describe('Non-GlossaryNode entity types', () => {
        const nonGlossaryEntityTypes = [
            EntityType.Domain,
            EntityType.Dataset,
            EntityType.DataProduct,
            EntityType.GlossaryTerm,
        ];

        it.each(nonGlossaryEntityTypes)('should set name to "Domains" for %s', (entityType) => {
            const result = getChildHierarchyModule(mockModule, testUrn, entityType);

            expect(result.properties.name).toBe('Domains');
        });

        it.each(nonGlossaryEntityTypes)('should set showRelatedEntities to false for %s', (entityType) => {
            const result = getChildHierarchyModule(mockModule, testUrn, entityType);

            expect(result.properties.params.hierarchyViewParams.showRelatedEntities).toBe(false);
        });

        it.each(nonGlossaryEntityTypes)('should set relatedEntitiesFilterJson to undefined for %s', (entityType) => {
            const result = getChildHierarchyModule(mockModule, testUrn, entityType);

            expect(result.properties.params.hierarchyViewParams.relatedEntitiesFilterJson).toBeUndefined();
        });

        it.each(nonGlossaryEntityTypes)(
            'should preserve existing module properties and params for %s',
            (entityType) => {
                const result = getChildHierarchyModule(mockModule, testUrn, entityType);

                expect(result.type).toBe(mockModule.type);
                expect(result.properties.params.hierarchyViewParams).toMatchObject({
                    assetUrns: [testUrn],
                    showRelatedEntities: false,
                });
            },
        );
    });

    describe('URN handling', () => {
        it('should correctly set assetUrns with provided URN', () => {
            const testUrns = [
                'urn:li:domain:test-domain',
                'urn:li:glossaryNode:test-node',
                'urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)',
            ];

            testUrns.forEach((urn) => {
                const result = getChildHierarchyModule(mockModule, urn, EntityType.Domain);
                expect(result.properties.params.hierarchyViewParams.assetUrns).toEqual([urn]);
            });
        });

        it('should handle empty URN', () => {
            const result = getChildHierarchyModule(mockModule, '', EntityType.Domain);
            expect(result.properties.params.hierarchyViewParams.assetUrns).toEqual(['']);
        });
    });

    describe('Module structure preservation', () => {
        it('should add hierarchyViewParams to existing params', () => {
            const result = getChildHierarchyModule(mockModule, testUrn, EntityType.Domain);

            expect(result.properties.params).toHaveProperty('hierarchyViewParams');
            expect(result.properties.params.hierarchyViewParams).toEqual({
                assetUrns: [testUrn],
                showRelatedEntities: false,
                relatedEntitiesFilterJson: undefined,
            });
        });

        it('should override existing hierarchyViewParams if present', () => {
            const moduleWithExistingHierarchy: PageModuleFragment = {
                urn: 'urn:li:dataHubPageModule:test1',
                type: EntityType.DatahubPageModule,
                properties: {
                    name: 'Original Name',
                    type: DataHubPageModuleType.ChildHierarchy,
                    visibility: { scope: PageModuleScope.Global },
                    params: {
                        hierarchyViewParams: { assetUrns: ['urn:li:domain:old-domain'], showRelatedEntities: true },
                    },
                },
            };

            const result = getChildHierarchyModule(moduleWithExistingHierarchy, testUrn, EntityType.Domain);

            expect(result.properties.params.hierarchyViewParams).toEqual({
                assetUrns: [testUrn],
                showRelatedEntities: false,
                relatedEntitiesFilterJson: undefined,
            });
        });
    });

    describe('Return value structure', () => {
        it('should return a new object (not mutate original)', () => {
            const originalModule = { ...mockModule };
            const result = getChildHierarchyModule(mockModule, testUrn, EntityType.Domain);

            expect(result).not.toBe(mockModule);
            expect(result.properties).not.toBe(mockModule.properties);
            expect(result.properties.params).not.toBe(mockModule.properties.params);
            expect(mockModule).toEqual(originalModule);
        });

        it('should have the correct structure', () => {
            const result = getChildHierarchyModule(mockModule, testUrn, EntityType.Domain);

            expect(result).toHaveProperty('type');
            expect(result).toHaveProperty('properties');
            expect(result.properties).toHaveProperty('name');
            expect(result.properties).toHaveProperty('params');
            expect(result.properties.params).toHaveProperty('hierarchyViewParams');
            expect(result.properties.params.hierarchyViewParams).toHaveProperty('assetUrns');
            expect(result.properties.params.hierarchyViewParams).toHaveProperty('showRelatedEntities');
            expect(result.properties.params.hierarchyViewParams).toHaveProperty('relatedEntitiesFilterJson');
        });
    });
});
