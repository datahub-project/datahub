import { afterEach, describe, expect, it, vi } from 'vitest';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';
import { assetPropertyToMenuItem, mapSummaryElement } from '@app/entityV2/summary/properties/utils';

import { SummaryElementFragment } from '@graphql/template.generated';
import { SummaryElementType } from '@types';

describe('assetPropertyToMenuItem', () => {
    const mockOnMenuItemClick = vi.fn();

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should convert an AssetProperty to a MenuItemType with key', () => {
        const assetProperty: AssetProperty = {
            key: 'testKey',
            type: PropertyType.Domain,
            name: 'Test Name',
            icon: 'testIcon',
        };

        const menuItem = assetPropertyToMenuItem(assetProperty, mockOnMenuItemClick);

        expect(menuItem.key).toBe('testKey');
        expect(menuItem.title).toBe('Test Name');
        expect(menuItem.icon).toBe('testIcon');
        expect(menuItem.type).toBe('item');
    });

    it('should use type as key if key is not provided', () => {
        const assetProperty: AssetProperty = {
            type: PropertyType.Domain,
            name: 'Test Name',
            icon: 'testIcon',
        };

        const menuItem = assetPropertyToMenuItem(assetProperty, mockOnMenuItemClick);

        expect(menuItem.key).toBe(PropertyType.Domain);
    });

    it('should call onMenuItemClick with the assetProperty when onClick is triggered', () => {
        const assetProperty: AssetProperty = {
            key: 'testKey',
            type: PropertyType.Domain,
            name: 'Test Name',
            icon: 'testIcon',
        };

        const menuItem = assetPropertyToMenuItem(assetProperty, mockOnMenuItemClick);
        menuItem.onClick?.();

        expect(mockOnMenuItemClick).toHaveBeenCalledWith(assetProperty);
    });
});

describe('mapSummaryElement', () => {
    const mockEntityRegistry: EntityRegistry = {
        getDisplayName: vi.fn(),
    } as any;

    afterEach(() => {
        vi.clearAllMocks();
    });

    describe('SUMMARY_ELEMENT_TYPE_TO_NAME mapping', () => {
        it('should map Created element type to "Created"', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.Created,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.name).toBe('Created');
            expect(result.type).toBe(SummaryElementType.Created);
            expect(result.structuredProperty).toBeUndefined();
        });

        it('should map Domain element type to "Domain"', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.Domain,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.name).toBe('Domain');
            expect(result.type).toBe(SummaryElementType.Domain);
            expect(result.structuredProperty).toBeUndefined();
        });

        it('should map GlossaryTerms element type to "Glossary Terms"', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.GlossaryTerms,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.name).toBe('Glossary Terms');
            expect(result.type).toBe(SummaryElementType.GlossaryTerms);
            expect(result.structuredProperty).toBeUndefined();
        });

        it('should map Owners element type to "Owners"', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.Owners,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.name).toBe('Owners');
            expect(result.type).toBe(SummaryElementType.Owners);
            expect(result.structuredProperty).toBeUndefined();
        });

        it('should map Tags element type to "Tags"', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.Tags,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.name).toBe('Tags');
            expect(result.type).toBe(SummaryElementType.Tags);
            expect(result.structuredProperty).toBeUndefined();
        });
    });

    describe('StructuredProperty handling', () => {
        it('should use entityRegistry.getDisplayName for StructuredProperty with valid structuredProperty', () => {
            const mockStructuredProperty = {
                __typename: 'StructuredPropertyEntity' as const,
                urn: 'urn:li:structuredProperty:test',
                type: 'STRING' as any,
                displayName: 'Test Property',
            };

            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: mockStructuredProperty,
            };

            (mockEntityRegistry.getDisplayName as any).mockReturnValue('Custom Display Name');

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(mockEntityRegistry.getDisplayName).toHaveBeenCalledWith(
                mockStructuredProperty.type,
                mockStructuredProperty,
            );
            expect(result.name).toBe('Custom Display Name');
            expect(result.type).toBe(SummaryElementType.StructuredProperty);
            expect(result.structuredProperty).toBe(mockStructuredProperty);
        });

        it('should fall back to element type when StructuredProperty but no structuredProperty provided', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(mockEntityRegistry.getDisplayName).not.toHaveBeenCalled();
            expect(result.name).toBe('STRUCTURED_PROPERTY');
            expect(result.type).toBe(SummaryElementType.StructuredProperty);
            expect(result.structuredProperty).toBeUndefined();
        });

        it('should fall back to element type when StructuredProperty but structuredProperty is undefined', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: undefined,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(mockEntityRegistry.getDisplayName).not.toHaveBeenCalled();
            expect(result.name).toBe('STRUCTURED_PROPERTY');
            expect(result.type).toBe(SummaryElementType.StructuredProperty);
            expect(result.structuredProperty).toBeUndefined();
        });
    });

    describe('Edge cases and fallback behavior', () => {
        it('should fall back to element type value when element type not in SUMMARY_ELEMENT_TYPE_TO_NAME', () => {
            // Create a mock element type that's not in the mapping
            const unknownElementType = 'UNKNOWN_TYPE' as SummaryElementType;

            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: unknownElementType,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.name).toBe('UNKNOWN_TYPE');
            expect(result.type).toBe(unknownElementType);
            expect(result.structuredProperty).toBeUndefined();
        });

        it('should handle structuredProperty with falsy values correctly', () => {
            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.Domain,
                structuredProperty: null,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.structuredProperty).toBeUndefined();
        });

        it('should preserve structuredProperty when provided for non-StructuredProperty types', () => {
            const mockStructuredProperty = {
                __typename: 'StructuredPropertyEntity' as const,
                urn: 'urn:li:structuredProperty:test',
                type: 'STRING' as any,
                displayName: 'Test Property',
            };

            const summaryElement: SummaryElementFragment = {
                __typename: 'SummaryElement',
                elementType: SummaryElementType.Domain,
                structuredProperty: mockStructuredProperty,
            };

            const result = mapSummaryElement(summaryElement, mockEntityRegistry);

            expect(result.name).toBe('Domain');
            expect(result.type).toBe(SummaryElementType.Domain);
            expect(result.structuredProperty).toBe(mockStructuredProperty);
            expect(mockEntityRegistry.getDisplayName).not.toHaveBeenCalled();
        });
    });
});
