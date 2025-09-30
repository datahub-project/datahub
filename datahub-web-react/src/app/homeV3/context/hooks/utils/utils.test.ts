import {
    filterNonExistentStructuredProperties,
    getDefaultSummaryPageTemplate,
} from '@app/homeV3/context/hooks/utils/utils';
import {
    ASSETS_MODULE,
    CHILD_HIERARCHY_MODULE,
    DATA_PRODUCTS_MODULE,
} from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';

import { EntityType, PageTemplateScope, PageTemplateSurfaceType, SummaryElementType } from '@types';

describe('getDefaultSummaryPageTemplate', () => {
    it('should return correct template for Domain entity type', () => {
        const result = getDefaultSummaryPageTemplate(EntityType.Domain);

        expect(result).toEqual({
            urn: 'urn:li:dataHubPageTemplate:asset_summary_default',
            type: EntityType.DatahubPageTemplate,
            properties: {
                visibility: {
                    scope: PageTemplateScope.Personal,
                },
                surface: {
                    surfaceType: PageTemplateSurfaceType.AssetSummary,
                },
                rows: [{ modules: [ASSETS_MODULE, CHILD_HIERARCHY_MODULE] }, { modules: [DATA_PRODUCTS_MODULE] }],
                assetSummary: {
                    summaryElements: [
                        { elementType: SummaryElementType.Created },
                        { elementType: SummaryElementType.Owners },
                    ],
                },
            },
        });

        // Verify modules array has content (but don't test specific content since it will change)
        expect(result.properties.rows[0].modules).toHaveLength(2);
    });

    it('should return correct template for DataProduct entity type', () => {
        const result = getDefaultSummaryPageTemplate(EntityType.DataProduct);

        expect(result).toEqual({
            urn: 'urn:li:dataHubPageTemplate:asset_summary_default',
            type: EntityType.DatahubPageTemplate,
            properties: {
                visibility: {
                    scope: PageTemplateScope.Personal,
                },
                surface: {
                    surfaceType: PageTemplateSurfaceType.AssetSummary,
                },
                rows: [{ modules: expect.any(Array) }],
                assetSummary: {
                    summaryElements: [
                        { elementType: SummaryElementType.Created },
                        { elementType: SummaryElementType.Owners },
                        { elementType: SummaryElementType.Domain },
                        { elementType: SummaryElementType.Tags },
                        { elementType: SummaryElementType.GlossaryTerms },
                    ],
                },
            },
        });

        // Verify modules array has content (but don't test specific content since it will change)
        expect(result.properties.rows[0].modules).toHaveLength(1);
    });

    it('should return correct template for GlossaryTerm entity type', () => {
        const result = getDefaultSummaryPageTemplate(EntityType.GlossaryTerm);

        expect(result).toEqual({
            urn: 'urn:li:dataHubPageTemplate:asset_summary_default',
            type: EntityType.DatahubPageTemplate,
            properties: {
                visibility: {
                    scope: PageTemplateScope.Personal,
                },
                surface: {
                    surfaceType: PageTemplateSurfaceType.AssetSummary,
                },
                rows: [{ modules: expect.any(Array) }],
                assetSummary: {
                    summaryElements: [
                        { elementType: SummaryElementType.Created },
                        { elementType: SummaryElementType.Owners },
                        { elementType: SummaryElementType.Domain },
                    ],
                },
            },
        });

        // Verify modules array has content (but don't test specific content since it will change)
        expect(result.properties.rows[0].modules).toHaveLength(2);
    });

    it('should return correct template for GlossaryNode entity type', () => {
        const result = getDefaultSummaryPageTemplate(EntityType.GlossaryNode);

        expect(result).toEqual({
            urn: 'urn:li:dataHubPageTemplate:asset_summary_default',
            type: EntityType.DatahubPageTemplate,
            properties: {
                visibility: {
                    scope: PageTemplateScope.Personal,
                },
                surface: {
                    surfaceType: PageTemplateSurfaceType.AssetSummary,
                },
                rows: [{ modules: expect.any(Array) }],
                assetSummary: {
                    summaryElements: [
                        { elementType: SummaryElementType.Created },
                        { elementType: SummaryElementType.Owners },
                    ],
                },
            },
        });

        // Verify modules array has content (but don't test specific content since it will change)
        expect(result.properties.rows[0].modules).toHaveLength(1);
    });

    it('should return template with empty arrays for unsupported entity types', () => {
        const result = getDefaultSummaryPageTemplate(EntityType.Dataset);

        expect(result).toEqual({
            urn: 'urn:li:dataHubPageTemplate:asset_summary_default',
            type: EntityType.DatahubPageTemplate,
            properties: {
                visibility: {
                    scope: PageTemplateScope.Personal,
                },
                surface: {
                    surfaceType: PageTemplateSurfaceType.AssetSummary,
                },
                rows: [{ modules: [] }],
                assetSummary: {
                    summaryElements: [],
                },
            },
        });
    });

    it('should always return consistent base template properties', () => {
        const entityTypes = [
            EntityType.DataProduct,
            EntityType.GlossaryTerm,
            EntityType.GlossaryNode,
            EntityType.Dataset, // unsupported type
        ];

        entityTypes.forEach((entityType) => {
            const result = getDefaultSummaryPageTemplate(entityType);

            // Test common properties that should be the same for all entity types
            expect(result.urn).toBe('urn:li:dataHubPageTemplate:asset_summary_default');
            expect(result.type).toBe(EntityType.DatahubPageTemplate);
            expect(result.properties.visibility.scope).toBe(PageTemplateScope.Personal);
            expect(result.properties.surface.surfaceType).toBe(PageTemplateSurfaceType.AssetSummary);
            if (entityType === EntityType.Domain) {
                expect(result.properties.rows).toHaveLength(2);
            } else {
                expect(result.properties.rows).toHaveLength(1);
            }
            expect(result.properties.rows[0]).toHaveProperty('modules');
            expect(result.properties.assetSummary).toHaveProperty('summaryElements');
        });
    });

    it('should return different summary elements for different entity types', () => {
        const domainResult = getDefaultSummaryPageTemplate(EntityType.Domain);
        const dataProductResult = getDefaultSummaryPageTemplate(EntityType.DataProduct);
        const glossaryTermResult = getDefaultSummaryPageTemplate(EntityType.GlossaryTerm);

        // Domain should have 2 elements
        expect(domainResult?.properties?.assetSummary?.summaryElements).toHaveLength(2);

        // DataProduct should have 5 elements (most comprehensive)
        expect(dataProductResult?.properties?.assetSummary?.summaryElements).toHaveLength(5);

        // GlossaryTerm should have 3 elements
        expect(glossaryTermResult?.properties?.assetSummary?.summaryElements).toHaveLength(3);

        // Verify they're actually different
        expect(domainResult?.properties?.assetSummary?.summaryElements).not.toEqual(
            dataProductResult?.properties?.assetSummary?.summaryElements,
        );
        expect(domainResult?.properties?.assetSummary?.summaryElements).not.toEqual(
            glossaryTermResult?.properties?.assetSummary?.summaryElements,
        );
    });
});

describe('filterNonExistentStructuredProperties', () => {
    it('should return empty array for empty input', () => {
        expect(filterNonExistentStructuredProperties([])).toEqual([]);
    });

    it('should keep all non-StructuredProperty elements', () => {
        const input = [{ elementType: SummaryElementType.Created }, { elementType: SummaryElementType.Domain }];
        expect(filterNonExistentStructuredProperties(input)).toEqual(input);
    });

    it('should keep StructuredProperty elements with exists=true', () => {
        const input = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:1',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        expect(filterNonExistentStructuredProperties(input)).toEqual(input);
    });

    it('should remove StructuredProperty elements with exists=false', () => {
        const input = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:1',
                    exists: false,
                    type: EntityType.StructuredProperty,
                },
            },
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        const expected = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        expect(filterNonExistentStructuredProperties(input)).toEqual(expected);
    });

    it('should remove StructuredProperty if structuredProperty is missing', () => {
        const input = [
            { elementType: SummaryElementType.StructuredProperty }, // no structuredProperty
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        const expected = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        expect(filterNonExistentStructuredProperties(input)).toEqual(expected);
    });

    it('should handle mixed elements keeping valid StructuredProperty and non-StructuredProperty', () => {
        const input = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:1',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
            { elementType: SummaryElementType.Created },
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: false,
                    type: EntityType.StructuredProperty,
                },
            },
            { elementType: SummaryElementType.Domain },
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:3',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        const expected = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:1',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
            { elementType: SummaryElementType.Created },
            { elementType: SummaryElementType.Domain },
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:3',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        expect(filterNonExistentStructuredProperties(input)).toEqual(expected);
    });

    it('should remove StructuredProperty when structuredProperty.exists is undefined', () => {
        const input = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:1',
                    exists: undefined as any,
                    type: EntityType.StructuredProperty,
                },
            },
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        const expected = [
            {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: {
                    urn: 'urn:2',
                    exists: true,
                    type: EntityType.StructuredProperty,
                },
            },
        ];
        expect(filterNonExistentStructuredProperties(input)).toEqual(expected);
    });
});
