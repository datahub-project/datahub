import { SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID } from '@app/tests/builder/steps/definition/builder/property/constants';
import { ActionId, ActionType } from '@app/tests/builder/steps/definition/builder/property/types/action';
import {
    LogicalOperatorType,
    LogicalPredicate,
    PropertyPredicate,
} from '@app/tests/builder/steps/definition/builder/types';
import {
    ASSET_CATEGORIES,
    filterActionTypesByEntities,
    getPropertiesFromLogicalPredicate,
    getValidationWarnings,
    isActionSupportedForEntities,
    isPropertySupportedForEntities,
    validateCompleteTestDefinition,
} from '@app/tests/builder/validation/utils';
import { TestDefinition } from '@app/tests/types';

import { EntityType } from '@types';

// Test data constants for better readability
const UNIVERSAL_PROPERTY = 'ownership.owners.owner';
const DATASET_SPECIFIC_PROPERTY = 'datasetProperties.description';
const MIXED_ENTITIES = [EntityType.Dataset, EntityType.GlossaryTerm];

/**
 * Comprehensive test suite for metadata test validation utilities.
 *
 * These tests ensure that:
 * - Property compatibility validation works correctly across entity types
 * - Action compatibility validation prevents unsupported operations
 * - Complex test definitions are validated end-to-end
 * - New entity types (Domain, DataProduct, GlossaryNode) are properly supported
 * - Column-level properties work only for datasets
 * - Edge cases are handled gracefully
 */
describe('Validation Utils', () => {
    // =============================================================================
    // PROPERTY SUPPORT VALIDATION
    // =============================================================================

    describe('isPropertySupportedForEntities', () => {
        it('should support universal properties for all entities', () => {
            // Universal properties work for any entity type
            expect(isPropertySupportedForEntities(UNIVERSAL_PROPERTY, [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities(UNIVERSAL_PROPERTY, [EntityType.GlossaryTerm])).toBe(true);
            expect(isPropertySupportedForEntities(UNIVERSAL_PROPERTY, MIXED_ENTITIES)).toBe(true);
        });

        it('should not support data entity specific properties for metadata entities', () => {
            // Dataset-specific properties only work for datasets
            expect(isPropertySupportedForEntities(DATASET_SPECIFIC_PROPERTY, [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities(DATASET_SPECIFIC_PROPERTY, [EntityType.GlossaryTerm])).toBe(false);
            expect(isPropertySupportedForEntities(DATASET_SPECIFIC_PROPERTY, MIXED_ENTITIES)).toBe(false);
        });

        it('should support data entity common properties for data entities only', () => {
            // Properties from baseEntityProps should work for data entities but not glossary terms
            // Let's use a property that's actually in baseEntityProps, not commonProps
            expect(isPropertySupportedForEntities('dataPlatformInstance.platform', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('dataPlatformInstance.platform', [EntityType.Chart])).toBe(true);
            expect(isPropertySupportedForEntities('dataPlatformInstance.platform', [EntityType.GlossaryTerm])).toBe(
                false,
            );
            expect(
                isPropertySupportedForEntities('dataPlatformInstance.platform', [
                    EntityType.Dataset,
                    EntityType.GlossaryTerm,
                ]),
            ).toBe(false);
        });

        it('should return true when no entities are selected', () => {
            expect(isPropertySupportedForEntities('any.property', [])).toBe(true);
        });
    });

    describe('isActionSupportedForEntities', () => {
        it('should support actions for compatible entity types', () => {
            // ADD_OWNERS should work for all entities
            expect(isActionSupportedForEntities(ActionId.ADD_OWNERS, [EntityType.Dataset])).toBe(true);
            expect(isActionSupportedForEntities(ActionId.ADD_OWNERS, [EntityType.GlossaryTerm])).toBe(true);
            expect(
                isActionSupportedForEntities(ActionId.ADD_OWNERS, [EntityType.Dataset, EntityType.GlossaryTerm]),
            ).toBe(true);
        });

        it('should not support actions for incompatible entity types', () => {
            // ADD_TAGS should not work for glossary terms
            expect(isActionSupportedForEntities(ActionId.ADD_TAGS, [EntityType.Dataset])).toBe(true);
            expect(isActionSupportedForEntities(ActionId.ADD_TAGS, [EntityType.GlossaryTerm])).toBe(false);
            expect(isActionSupportedForEntities(ActionId.ADD_TAGS, [EntityType.Dataset, EntityType.GlossaryTerm])).toBe(
                false,
            );
        });

        it('should return true when no entities are selected', () => {
            expect(isActionSupportedForEntities(ActionId.ADD_TAGS, [])).toBe(true);
        });
    });

    describe('getPropertiesFromLogicalPredicate', () => {
        it('should extract properties from a simple property predicate', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'datasetProperties.description',
                operator: 'exists',
            };

            expect(getPropertiesFromLogicalPredicate(predicate)).toEqual(['datasetProperties.description']);
        });

        it('should extract properties from a logical predicate with AND operator', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: 'datasetProperties.description',
                        operator: 'exists',
                    },
                    {
                        type: 'property',
                        property: 'ownership.owners.owner',
                        operator: 'exists',
                    },
                ],
            };

            const properties = getPropertiesFromLogicalPredicate(predicate);
            expect(properties).toHaveLength(2);
            expect(properties).toContain('datasetProperties.description');
            expect(properties).toContain('ownership.owners.owner');
        });

        it('should extract unique properties from nested predicates', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.OR,
                operands: [
                    {
                        type: 'property',
                        property: 'datasetProperties.description',
                        operator: 'exists',
                    },
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property',
                                property: 'datasetProperties.description', // Duplicate
                                operator: 'equals',
                                values: ['test'],
                            },
                            {
                                type: 'property',
                                property: 'ownership.owners.owner',
                                operator: 'exists',
                            },
                        ],
                    },
                ],
            };

            const properties = getPropertiesFromLogicalPredicate(predicate);
            expect(properties).toHaveLength(2); // Should deduplicate
            expect(properties).toContain('datasetProperties.description');
            expect(properties).toContain('ownership.owners.owner');
        });

        it('should return empty array for null predicate', () => {
            expect(getPropertiesFromLogicalPredicate(null)).toEqual([]);
        });

        it('should handle property predicates without property field', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                operator: 'exists',
            };

            expect(getPropertiesFromLogicalPredicate(predicate)).toEqual([]);
        });
    });

    describe('getValidationWarnings', () => {
        it('should return empty warnings for valid configurations', () => {
            // Valid: Dataset with dataset-specific property
            const warnings1 = getValidationWarnings([EntityType.Dataset], ['datasetProperties.description'], []);
            expect(warnings1).toHaveLength(0);

            // Valid: All entities with universal property
            const warnings2 = getValidationWarnings(
                [EntityType.Dataset, EntityType.GlossaryTerm],
                ['ownership.owners.owner'],
                [],
            );
            expect(warnings2).toHaveLength(0);

            // Valid: Data entities with compatible action
            const warnings3 = getValidationWarnings(
                [EntityType.Dataset, EntityType.Chart],
                [],
                [{ id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' }],
            );
            expect(warnings3).toHaveLength(0);
        });

        it('should return warnings for invalid property configurations', () => {
            // Invalid: Glossary term with dataset-specific property
            const warnings = getValidationWarnings(
                [EntityType.Dataset, EntityType.GlossaryTerm],
                ['datasetProperties.description'],
                [],
            );

            expect(warnings).toHaveLength(1);
            expect(warnings[0].type).toBe('property');
            expect(warnings[0].message).toContain('datasetProperties.description');
            expect(warnings[0].message).toContain('glossary terms');
            expect(warnings[0].propertyId).toBe('datasetProperties.description');
        });

        it('should return warnings for invalid action configurations', () => {
            // Invalid: Glossary term with ADD_TAGS action
            const warnings = getValidationWarnings(
                [EntityType.Dataset, EntityType.GlossaryTerm],
                [],
                [{ id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' }],
            );

            expect(warnings).toHaveLength(1);
            expect(warnings[0].type).toBe('action');
            expect(warnings[0].message).toContain('Add Tags');
            expect(warnings[0].message).toContain('glossary terms');
            expect(warnings[0].actionId).toBe(ActionId.ADD_TAGS);
        });

        it('should return multiple warnings for multiple invalid configurations', () => {
            // Multiple invalid: Glossary term with dataset property and ADD_TAGS action
            const warnings = getValidationWarnings(
                [EntityType.Dataset, EntityType.GlossaryTerm],
                ['datasetProperties.description'],
                [{ id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' }],
            );

            expect(warnings).toHaveLength(2);

            const propertyWarning = warnings.find((w) => w.type === 'property');
            const actionWarning = warnings.find((w) => w.type === 'action');

            expect(propertyWarning).toBeDefined();
            expect(propertyWarning?.propertyId).toBe('datasetProperties.description');

            expect(actionWarning).toBeDefined();
            expect(actionWarning?.actionId).toBe(ActionId.ADD_TAGS);
        });

        it('should return empty warnings when no entities are selected', () => {
            const warnings = getValidationWarnings(
                [],
                ['datasetProperties.description'],
                [{ id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' }],
            );
            expect(warnings).toHaveLength(0);
        });

        it('should return empty warnings when no properties or actions are provided', () => {
            const warnings = getValidationWarnings([EntityType.Dataset, EntityType.GlossaryTerm], [], []);
            expect(warnings).toHaveLength(0);
        });

        it('should format entity names properly in warning messages', () => {
            const warnings = getValidationWarnings([EntityType.GlossaryTerm], ['datasetProperties.description'], []);

            expect(warnings).toHaveLength(1);
            expect(warnings[0].message).toContain('glossary terms'); // Properly formatted and pluralized
            expect(warnings[0].message).not.toContain('glossary_term'); // Should not contain underscores
            expect(warnings[0].message).not.toContain('GLOSSARY_TERM'); // Should not be uppercase
        });

        it('should handle complex entity combinations', () => {
            // Test with multiple entity types where some support the property and some don't
            const warnings = getValidationWarnings(
                [EntityType.Dataset, EntityType.Chart, EntityType.GlossaryTerm],
                ['datasetProperties.description'], // Only supported by Dataset
                [],
            );

            expect(warnings).toHaveLength(1);
            expect(warnings[0].message).toContain('glossary terms'); // Should only mention unsupported entities
        });

        it('should support new column-related properties for datasets only', () => {
            // Test new column name property (using correct searchable field name)
            expect(isPropertySupportedForEntities('fieldPaths', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('fieldPaths', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('fieldPaths', [EntityType.GlossaryTerm])).toBe(false);

            // Test column tags (using correct searchable field names)
            expect(isPropertySupportedForEntities('fieldTags', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('fieldTags', [EntityType.Dashboard])).toBe(false);

            // Test grouped column properties
            expect(isPropertySupportedForEntities('columnTags', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('columnTags', [EntityType.Chart])).toBe(false);

            expect(isPropertySupportedForEntities('columnDescriptions', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('columnDescriptions', [EntityType.Dashboard])).toBe(false);

            expect(isPropertySupportedForEntities('columnGlossaryTerms', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('columnGlossaryTerms', [EntityType.GlossaryTerm])).toBe(false);
        });

        it('should generate warnings for column properties used with non-dataset entities', () => {
            // Test column names property warning
            const warnings1 = getValidationWarnings(
                [EntityType.Chart, EntityType.Dashboard],
                ['schemaMetadata.fields.fieldPath'],
                [],
            );
            expect(warnings1).toHaveLength(1);
            expect(warnings1[0].type).toBe('property');
            expect(warnings1[0].message).toContain('schemaMetadata.fields.fieldPath');
            expect(warnings1[0].message).toContain('charts');
            expect(warnings1[0].message).toContain('dashboards');

            // Test column data types property warning
            const warnings2 = getValidationWarnings(
                [EntityType.GlossaryTerm],
                ['schemaMetadata.fields.nativeDataType'],
                [],
            );
            expect(warnings2).toHaveLength(1);
            expect(warnings2[0].message).toContain('schemaMetadata.fields.nativeDataType');
            expect(warnings2[0].message).toContain('glossary terms');

            // Test grouped column properties warning
            const warnings3 = getValidationWarnings([EntityType.Dataset, EntityType.Chart], ['columnTags'], []);
            expect(warnings3).toHaveLength(1);
            expect(warnings3[0].message).toContain('columnTags');
            expect(warnings3[0].message).toContain('charts');
            expect(warnings3[0].message).not.toContain('datasets'); // Should not mention supported entities
        });

        it('should support schema-level constraint properties for datasets only', () => {
            // Test primary keys property
            expect(isPropertySupportedForEntities('schemaMetadata.primaryKeys', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('schemaMetadata.primaryKeys', [EntityType.Chart])).toBe(false);

            // Test foreign keys properties
            expect(isPropertySupportedForEntities('schemaMetadata.foreignKeys', [EntityType.Dataset])).toBe(true);
            expect(
                isPropertySupportedForEntities('schemaMetadata.foreignKeys.foreignDataset', [EntityType.Dataset]),
            ).toBe(true);
            expect(
                isPropertySupportedForEntities('schemaMetadata.foreignKeys.foreignFields', [EntityType.Dataset]),
            ).toBe(true);

            // These should not work for non-dataset entities
            expect(isPropertySupportedForEntities('schemaMetadata.primaryKeys', [EntityType.Dashboard])).toBe(false);
            expect(isPropertySupportedForEntities('schemaMetadata.foreignKeys', [EntityType.GlossaryTerm])).toBe(false);

            // Note: Column structured properties are not yet supported in the metadata test framework
        });

        it('should support properties for new entity types (Domain, DataProduct, GlossaryNode)', () => {
            // Test Domain-specific properties
            expect(isPropertySupportedForEntities('domainProperties.name', [EntityType.Domain])).toBe(true);
            expect(isPropertySupportedForEntities('domainProperties.description', [EntityType.Domain])).toBe(true);
            expect(isPropertySupportedForEntities('domainProperties.parentDomain', [EntityType.Domain])).toBe(true);
            expect(isPropertySupportedForEntities('domainProperties.name', [EntityType.Dataset])).toBe(false);

            // Test DataProduct properties (should inherit from assetProps)
            expect(isPropertySupportedForEntities('ownership.owners.owner', [EntityType.DataProduct])).toBe(true);
            expect(isPropertySupportedForEntities('globalTags.tags.tag', [EntityType.DataProduct])).toBe(true);
            expect(isPropertySupportedForEntities('glossaryTerms.terms.urn', [EntityType.DataProduct])).toBe(true);

            // Test GlossaryNode-specific properties
            expect(isPropertySupportedForEntities('glossaryNodeInfo.name', [EntityType.GlossaryNode])).toBe(true);
            expect(isPropertySupportedForEntities('glossaryNodeInfo.definition', [EntityType.GlossaryNode])).toBe(true);
            expect(isPropertySupportedForEntities('glossaryNodeInfo.parentNode', [EntityType.GlossaryNode])).toBe(true);
            expect(isPropertySupportedForEntities('glossaryNodeInfo.name', [EntityType.GlossaryTerm])).toBe(false);

            // Test that dataset-specific properties don't work for new entities
            expect(isPropertySupportedForEntities('datasetProperties.description', [EntityType.Domain])).toBe(false);
            expect(isPropertySupportedForEntities('datasetProperties.description', [EntityType.DataProduct])).toBe(
                false,
            );
            expect(isPropertySupportedForEntities('datasetProperties.description', [EntityType.GlossaryNode])).toBe(
                false,
            );
        });

        it('should generate warnings for new entity types with incompatible properties', () => {
            // Test Domain with dataset property
            const warnings1 = getValidationWarnings([EntityType.Domain], ['schemaMetadata.fields.fieldPath'], []);
            expect(warnings1).toHaveLength(1);
            expect(warnings1[0].message).toContain('schemaMetadata.fields.fieldPath');
            expect(warnings1[0].message).toContain('domains');

            // Test mixed entities with dataset-specific property
            const warnings2 = getValidationWarnings(
                [EntityType.Dataset, EntityType.DataProduct, EntityType.GlossaryNode],
                ['columnDescriptions'],
                [],
            );
            expect(warnings2).toHaveLength(1);
            expect(warnings2[0].message).toContain('columnDescriptions');
            expect(warnings2[0].message).toContain('data products');
            expect(warnings2[0].message).toContain('glossary nodes');
            expect(warnings2[0].message).not.toContain('datasets'); // Should not mention supported entities
        });
    });

    describe('Entity name formatting', () => {
        it('should format entity names correctly in warning messages', () => {
            // Test various entity types to ensure proper formatting
            const testCases = [
                { entity: EntityType.GlossaryTerm, expected: 'glossary terms' },
                { entity: EntityType.Chart, expected: 'charts' },
                { entity: EntityType.Dashboard, expected: 'dashboards' },
            ];

            testCases.forEach(({ entity, expected }) => {
                const warnings = getValidationWarnings(
                    [entity],
                    ['datasetProperties.description'], // This should be invalid for non-dataset entities
                    [],
                );

                // All non-dataset entities should get warnings for dataset-specific properties
                expect(warnings).toHaveLength(1);
                expect(warnings[0].message).toContain(expected);
                expect(warnings[0].message).toContain('datasetProperties.description');
            });
        });
    });

    describe('validateCompleteTestDefinition', () => {
        it('should validate test definitions with selection filters and rules', () => {
            const testDefinition: TestDefinition = {
                on: {
                    types: ['DATASET'],
                    conditions: [
                        {
                            property: 'datasetProperties.description',
                            operator: 'exists',
                        },
                    ],
                },
                rules: [
                    {
                        property: 'ownership.owners.owner',
                        operator: 'exists',
                    },
                ],
                actions: {
                    passing: [{ type: ActionId.ADD_TAGS, values: [] }],
                    failing: [{ type: ActionId.DEPRECATE, values: [] }],
                },
            };

            // Valid for datasets
            const warnings1 = validateCompleteTestDefinition([EntityType.Dataset], testDefinition);
            expect(warnings1).toHaveLength(0);

            // Invalid for glossary terms (dataset-specific property in selection)
            const warnings2 = validateCompleteTestDefinition([EntityType.GlossaryTerm], testDefinition);
            expect(warnings2.length).toBeGreaterThan(0);
            expect(warnings2.some((w) => w.propertyId === 'datasetProperties.description')).toBe(true);
            expect(warnings2.some((w) => w.actionId === ActionId.ADD_TAGS)).toBe(true);
        });

        it('should handle test definitions without selection filters', () => {
            const testDefinition: TestDefinition = {
                on: {
                    types: ['DATASET', 'GLOSSARY_TERM'],
                },
                rules: [
                    {
                        property: 'ownership.owners.owner',
                        operator: 'exists',
                    },
                ],
                actions: {
                    passing: [{ type: ActionId.ADD_OWNERS, values: [] }],
                    failing: [],
                },
            };

            const warnings = validateCompleteTestDefinition(
                [EntityType.Dataset, EntityType.GlossaryTerm],
                testDefinition,
            );
            expect(warnings).toHaveLength(0); // Universal property and action should work
        });

        it('should handle test definitions without actions', () => {
            const testDefinition: TestDefinition = {
                on: {
                    types: ['DATASET'],
                },
                rules: [
                    {
                        property: 'datasetProperties.description',
                        operator: 'exists',
                    },
                ],
            };

            const warnings1 = validateCompleteTestDefinition([EntityType.Dataset], testDefinition);
            expect(warnings1).toHaveLength(0);

            const warnings2 = validateCompleteTestDefinition([EntityType.GlossaryTerm], testDefinition);
            expect(warnings2).toHaveLength(1);
            expect(warnings2[0].propertyId).toBe('datasetProperties.description');
        });

        it('should extract properties from complex nested logical predicates', () => {
            const testDefinition: TestDefinition = {
                on: {
                    types: ['CHART', 'DASHBOARD'],
                    conditions: [
                        {
                            or: [
                                {
                                    property: 'datasetProperties.description',
                                    operator: 'exists',
                                },
                                {
                                    property: 'schemaMetadata.fields.fieldPath',
                                    operator: 'exists',
                                },
                            ],
                        },
                    ],
                },
                rules: [
                    {
                        property: 'columnTags',
                        operator: 'exists',
                    },
                ],
            };

            const warnings = validateCompleteTestDefinition([EntityType.Chart, EntityType.Dashboard], testDefinition);
            expect(warnings).toHaveLength(3); // All three dataset-specific properties should generate warnings

            const propertyIds = warnings.map((w) => w.propertyId).filter(Boolean);
            expect(propertyIds).toContain('datasetProperties.description');
            expect(propertyIds).toContain('schemaMetadata.fields.fieldPath');
            expect(propertyIds).toContain('columnTags');
        });
    });

    describe('filterActionTypesByEntities', () => {
        it('should filter actions based on entity compatibility', () => {
            const allActions: ActionType[] = [
                { id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' },
                { id: ActionId.ADD_OWNERS, displayName: 'Add Owners', description: 'Add owners to entities' },
                { id: ActionId.DEPRECATE, displayName: 'Deprecate', description: 'Mark entities as deprecated' },
                { id: ActionId.SET_DOMAIN, displayName: 'Set Domain', description: 'Set domain for entities' },
                {
                    id: ActionId.SET_DATA_PRODUCT,
                    displayName: 'Set Data Product',
                    description: 'Set data product for entities',
                },
                {
                    id: ActionId.UNSET_DATA_PRODUCT,
                    displayName: 'Remove Data Product',
                    description: 'Remove data product for entities',
                },
            ];

            // For datasets - should support all actions including data product actions
            const datasetActions = filterActionTypesByEntities(allActions, [EntityType.Dataset]);
            expect(datasetActions).toHaveLength(6); // All 6 actions should be supported for datasets

            // For glossary terms - should exclude ADD_TAGS and data product actions (logical entities don't support data product actions)
            const glossaryActions = filterActionTypesByEntities(allActions, [EntityType.GlossaryTerm]);
            expect(glossaryActions).toHaveLength(3); // ADD_OWNERS, DEPRECATE, and SET_DOMAIN
            expect(glossaryActions.some((a) => a.id === ActionId.ADD_TAGS)).toBe(false);
            expect(glossaryActions.some((a) => a.id === ActionId.SET_DATA_PRODUCT)).toBe(false);
            expect(glossaryActions.some((a) => a.id === ActionId.UNSET_DATA_PRODUCT)).toBe(false);
            expect(glossaryActions.some((a) => a.id === ActionId.ADD_OWNERS)).toBe(true);
            expect(glossaryActions.some((a) => a.id === ActionId.SET_DOMAIN)).toBe(true);

            // For mixed entities - should only include actions supported by all
            const mixedActions = filterActionTypesByEntities(allActions, [EntityType.Dataset, EntityType.GlossaryTerm]);
            expect(mixedActions.length).toBeLessThan(6);
            expect(mixedActions.some((a) => a.id === ActionId.ADD_TAGS)).toBe(false);
            expect(mixedActions.some((a) => a.id === ActionId.SET_DATA_PRODUCT)).toBe(false);
        });

        it('should return all actions when no entities are selected', () => {
            const allActions: ActionType[] = [
                { id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' },
                { id: ActionId.ADD_OWNERS, displayName: 'Add Owners', description: 'Add owners to entities' },
            ];

            const filteredActions = filterActionTypesByEntities(allActions, []);
            expect(filteredActions).toHaveLength(2);
        });

        it('should handle empty action list', () => {
            const filteredActions = filterActionTypesByEntities([], [EntityType.Dataset]);
            expect(filteredActions).toHaveLength(0);
        });
    });

    describe('ASSET_CATEGORIES', () => {
        it('should correctly categorize data assets vs logical entities', () => {
            // Data assets should include datasets, charts, dashboards, etc.
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.Dataset)).toBe(true);
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.Chart)).toBe(true);
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.Dashboard)).toBe(true);
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.DataFlow)).toBe(true);
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.DataJob)).toBe(true);
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.Container)).toBe(true);

            // Note: DataProduct is conceptually a logical asset but functionally categorized as a data asset
            // due to inheriting from assetProps (which includes platform, tags, glossary terms, etc.)
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.DataProduct)).toBe(true);

            // Metadata entities should include glossary terms, domains, etc.
            expect(ASSET_CATEGORIES.METADATA_ASSETS.has(EntityType.GlossaryTerm)).toBe(true);
            expect(ASSET_CATEGORIES.METADATA_ASSETS.has(EntityType.Domain)).toBe(true);
            expect(ASSET_CATEGORIES.METADATA_ASSETS.has(EntityType.GlossaryNode)).toBe(true);

            // Entities should not be in both categories
            ASSET_CATEGORIES.DATA_ASSETS.forEach((entity) => {
                expect(ASSET_CATEGORIES.METADATA_ASSETS.has(entity)).toBe(false);
            });

            ASSET_CATEGORIES.METADATA_ASSETS.forEach((entity) => {
                expect(ASSET_CATEGORIES.DATA_ASSETS.has(entity)).toBe(false);
            });
        });

        it('should include all newly added entity types', () => {
            // Verify our new entity types are properly categorized
            expect(ASSET_CATEGORIES.DATA_ASSETS.has(EntityType.DataProduct)).toBe(true);
            expect(ASSET_CATEGORIES.METADATA_ASSETS.has(EntityType.Domain)).toBe(true);
            expect(ASSET_CATEGORIES.METADATA_ASSETS.has(EntityType.GlossaryNode)).toBe(true);
        });
    });

    describe('Property extraction and categorization', () => {
        it('should correctly identify universal vs entity-specific properties', () => {
            // Universal properties should work for all entities
            expect(
                isPropertySupportedForEntities('ownership.owners.owner', [
                    EntityType.Dataset,
                    EntityType.GlossaryTerm,
                    EntityType.Domain,
                ]),
            ).toBe(true);
            expect(
                isPropertySupportedForEntities('globalTags.tags.tag', [EntityType.Chart, EntityType.DataProduct]),
            ).toBe(true);

            // Base entity properties should work for data assets but not logical entities
            expect(
                isPropertySupportedForEntities('dataPlatformInstance.platform', [EntityType.Dataset, EntityType.Chart]),
            ).toBe(true);
            expect(
                isPropertySupportedForEntities('dataPlatformInstance.platform', [
                    EntityType.GlossaryTerm,
                    EntityType.Domain,
                ]),
            ).toBe(false);

            // Entity-specific properties should only work for their specific entity
            expect(isPropertySupportedForEntities('datasetProperties.description', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('datasetProperties.description', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('domainProperties.name', [EntityType.Domain])).toBe(true);
            expect(isPropertySupportedForEntities('domainProperties.name', [EntityType.Dataset])).toBe(false);
        });

        it('should support ownership type properties for data assets only', () => {
            // Test base ownership types property (from assetProps)
            expect(isPropertySupportedForEntities('ownership.owners.typeUrn', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('ownership.owners.typeUrn', [EntityType.Chart])).toBe(true);
            expect(isPropertySupportedForEntities('ownership.owners.typeUrn', [EntityType.DataProduct])).toBe(true);

            // Should not work for logical assets
            expect(isPropertySupportedForEntities('ownership.owners.typeUrn', [EntityType.GlossaryTerm])).toBe(false);
            expect(isPropertySupportedForEntities('ownership.owners.typeUrn', [EntityType.Domain])).toBe(false);

            // Test dynamic ownership type properties (created by OwnershipTypePredicateBuilder)
            const businessOwnerProperty = 'ownership.ownerTypes.urn:li:ownershipType:__system__business_owner';
            const technicalOwnerProperty = 'ownership.ownerTypes.urn:li:ownershipType:__system__technical_owner';

            // Should work for all data assets
            expect(isPropertySupportedForEntities(businessOwnerProperty, [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities(businessOwnerProperty, [EntityType.Dashboard])).toBe(true);
            expect(isPropertySupportedForEntities(technicalOwnerProperty, [EntityType.Container])).toBe(true);

            // Should not work for logical assets
            expect(isPropertySupportedForEntities(businessOwnerProperty, [EntityType.GlossaryTerm])).toBe(false);
            expect(isPropertySupportedForEntities(technicalOwnerProperty, [EntityType.GlossaryNode])).toBe(false);
        });

        it('should support structured properties for data assets only', () => {
            // Test placeholder structured property
            const structuredPropertyPlaceholder = '__structuredPropertyRef';
            expect(isPropertySupportedForEntities(structuredPropertyPlaceholder, [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities(structuredPropertyPlaceholder, [EntityType.Chart])).toBe(true);
            expect(isPropertySupportedForEntities(structuredPropertyPlaceholder, [EntityType.DataProduct])).toBe(true);

            // Should not work for logical assets
            expect(isPropertySupportedForEntities(structuredPropertyPlaceholder, [EntityType.GlossaryTerm])).toBe(
                false,
            );
            expect(isPropertySupportedForEntities(structuredPropertyPlaceholder, [EntityType.Domain])).toBe(false);

            // Test dynamic structured properties (created by StructuredPropertyPredicateBuilder)
            const customStructuredProperty = 'structuredProperties.urn:li:structuredProperty:my_custom_property';
            const anotherStructuredProperty = 'structuredProperties.urn:li:structuredProperty:business_critical';

            // Should work for all data assets
            expect(isPropertySupportedForEntities(customStructuredProperty, [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities(customStructuredProperty, [EntityType.Dashboard])).toBe(true);
            expect(isPropertySupportedForEntities(anotherStructuredProperty, [EntityType.Container])).toBe(true);

            // Should not work for logical assets
            expect(isPropertySupportedForEntities(customStructuredProperty, [EntityType.GlossaryTerm])).toBe(false);
            expect(isPropertySupportedForEntities(anotherStructuredProperty, [EntityType.GlossaryNode])).toBe(false);
        });

        it('should handle complex property hierarchies', () => {
            // Test nested property groups we added
            expect(isPropertySupportedForEntities('columnTags', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('columnDescriptions', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('columnGlossaryTerms', [EntityType.Dataset])).toBe(true);

            // These should not work for other entities
            expect(isPropertySupportedForEntities('columnTags', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('columnDescriptions', [EntityType.Dashboard])).toBe(false);
        });

        it('should properly extract properties from nested logical predicates', () => {
            const complexPredicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property',
                                property: 'datasetProperties.description',
                                operator: 'exists',
                            },
                            {
                                type: 'property',
                                property: 'schemaMetadata.fields.fieldPath',
                                operator: 'exists',
                            },
                        ],
                    },
                    {
                        type: 'property',
                        property: 'ownership.owners.owner',
                        operator: 'exists',
                    },
                ],
            };

            const properties = getPropertiesFromLogicalPredicate(complexPredicate);
            expect(properties).toHaveLength(3);
            expect(properties).toContain('datasetProperties.description');
            expect(properties).toContain('schemaMetadata.fields.fieldPath');
            expect(properties).toContain('ownership.owners.owner');
        });
    });

    describe('Warning message formatting and content', () => {
        it('should generate properly formatted warning messages for properties', () => {
            const warnings = getValidationWarnings(
                [EntityType.Chart, EntityType.Dashboard, EntityType.GlossaryTerm],
                ['datasetProperties.description'],
                [],
            );

            expect(warnings).toHaveLength(1);
            const warning = warnings[0];

            expect(warning.type).toBe('property');
            expect(warning.message).toContain('datasetProperties.description');
            expect(warning.message).toContain('charts');
            expect(warning.message).toContain('dashboards');
            expect(warning.message).toContain('glossary terms');
            expect(warning.propertyId).toBe('datasetProperties.description');
        });

        it('should generate properly formatted warning messages for actions', () => {
            const warnings = getValidationWarnings(
                [EntityType.GlossaryTerm, EntityType.Domain],
                [],
                [{ id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' }],
            );

            expect(warnings).toHaveLength(1);
            const warning = warnings[0];

            expect(warning.type).toBe('action');
            expect(warning.message).toContain('Add Tags');
            expect(warning.message).toContain('glossary terms');
            expect(warning.message).toContain('domains');
            expect(warning.actionId).toBe(ActionId.ADD_TAGS);
        });

        it('should handle edge cases in warning generation', () => {
            // Single entity type
            const warnings1 = getValidationWarnings([EntityType.GlossaryTerm], ['datasetProperties.description'], []);
            expect(warnings1[0].message).toContain('glossary terms');
            expect(warnings1[0].message).not.toContain('and'); // Should not have "and" for single entity

            // Multiple warnings
            const warnings2 = getValidationWarnings(
                [EntityType.GlossaryTerm],
                ['datasetProperties.description', 'fieldPaths'], // Updated to use correct field name
                [{ id: ActionId.ADD_TAGS, displayName: 'Add Tags', description: 'Add tags to entities' }],
            );
            expect(warnings2).toHaveLength(3); // 2 property warnings + 1 action warning
        });
    });

    // =============================================================================
    // SCHEMA FIELD PROPERTIES (CORRECTED SEARCHABLE FIELD NAMES)
    // =============================================================================

    describe('Schema field properties with correct searchable field names', () => {
        it('should support all schema field glossary term properties for datasets only', () => {
            // Platform (read-only) schema field glossary terms
            expect(isPropertySupportedForEntities('fieldGlossaryTerms', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('fieldGlossaryTerms', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('fieldGlossaryTerms', [EntityType.Dashboard])).toBe(false);
            expect(isPropertySupportedForEntities('fieldGlossaryTerms', [EntityType.GlossaryTerm])).toBe(false);

            // DataHub editable schema field glossary terms
            expect(isPropertySupportedForEntities('editedFieldGlossaryTerms', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('editedFieldGlossaryTerms', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('editedFieldGlossaryTerms', [EntityType.Dashboard])).toBe(false);
        });

        it('should support all schema field tag properties for datasets only', () => {
            // Platform (read-only) schema field tags
            expect(isPropertySupportedForEntities('fieldTags', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('fieldTags', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('fieldTags', [EntityType.Dashboard])).toBe(false);

            // DataHub editable schema field tags
            expect(isPropertySupportedForEntities('editedFieldTags', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('editedFieldTags', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('editedFieldTags', [EntityType.Dashboard])).toBe(false);
        });

        it('should support all schema field description properties for datasets only', () => {
            // Platform (read-only) schema field descriptions
            expect(isPropertySupportedForEntities('fieldDescriptions', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('fieldDescriptions', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('fieldDescriptions', [EntityType.Dashboard])).toBe(false);

            // DataHub editable schema field descriptions
            expect(isPropertySupportedForEntities('editedFieldDescriptions', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('editedFieldDescriptions', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('editedFieldDescriptions', [EntityType.Dashboard])).toBe(false);
        });

        it('should support schema field paths for datasets only', () => {
            // Field paths (column names)
            expect(isPropertySupportedForEntities('fieldPaths', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('fieldPaths', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('fieldPaths', [EntityType.Dashboard])).toBe(false);
            expect(isPropertySupportedForEntities('fieldPaths', [EntityType.GlossaryTerm])).toBe(false);
        });

        it('should support schema field structured properties for datasets only', () => {
            // Schema field structured property placeholder
            expect(
                isPropertySupportedForEntities(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID, [
                    EntityType.Dataset,
                ]),
            ).toBe(true);
            expect(
                isPropertySupportedForEntities(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID, [
                    EntityType.Chart,
                ]),
            ).toBe(false);
            expect(
                isPropertySupportedForEntities(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID, [
                    EntityType.Dashboard,
                ]),
            ).toBe(false);
            expect(
                isPropertySupportedForEntities(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID, [
                    EntityType.GlossaryTerm,
                ]),
            ).toBe(false);

            // Specific schema field structured property with URN
            const schemaFieldStructuredPropertyId =
                'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';
            expect(isPropertySupportedForEntities(schemaFieldStructuredPropertyId, [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities(schemaFieldStructuredPropertyId, [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities(schemaFieldStructuredPropertyId, [EntityType.Dashboard])).toBe(false);
            expect(isPropertySupportedForEntities(schemaFieldStructuredPropertyId, [EntityType.GlossaryTerm])).toBe(
                false,
            );
        });

        it('should generate appropriate warnings for schema field properties on non-datasets', () => {
            // Test warnings for schema field glossary terms
            const glossaryTermWarnings = getValidationWarnings(
                [EntityType.Chart, EntityType.Dashboard],
                ['fieldGlossaryTerms', 'editedFieldGlossaryTerms'],
                [],
            );
            expect(glossaryTermWarnings).toHaveLength(2);
            expect(glossaryTermWarnings[0].message).toContain('fieldGlossaryTerms');
            expect(glossaryTermWarnings[0].message).toContain('charts, dashboards');
            expect(glossaryTermWarnings[1].message).toContain('editedFieldGlossaryTerms');

            // Test warnings for schema field tags
            const tagWarnings = getValidationWarnings([EntityType.GlossaryTerm], ['fieldTags', 'editedFieldTags'], []);
            expect(tagWarnings).toHaveLength(2);
            expect(tagWarnings[0].message).toContain('fieldTags');
            expect(tagWarnings[0].message).toContain('glossary terms');

            // Test warnings for schema field structured properties
            const structuredPropertyWarnings = getValidationWarnings(
                [EntityType.Chart, EntityType.Dashboard],
                [
                    SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:test',
                ],
                [],
            );
            expect(structuredPropertyWarnings).toHaveLength(2);
            expect(structuredPropertyWarnings[0].message).toContain('Column Structured Property');
            expect(structuredPropertyWarnings[0].message).toContain('charts, dashboards');
            expect(structuredPropertyWarnings[1].message).toContain('schemaFieldStructuredProperties');
            expect(structuredPropertyWarnings[1].message).toContain('charts, dashboards');
        });

        it('should generate appropriate warnings for ownership type and structured property conflicts', () => {
            // Test ownership type warnings
            const ownershipTypeWarnings = getValidationWarnings(
                [EntityType.GlossaryTerm, EntityType.Domain],
                ['ownership.ownerTypes.urn:li:ownershipType:__system__business_owner'],
                [],
            );
            expect(ownershipTypeWarnings).toHaveLength(1);
            expect(ownershipTypeWarnings[0].message).toContain('Ownership Type');
            expect(ownershipTypeWarnings[0].message).toContain('glossary terms, domains');
            expect(ownershipTypeWarnings[0].message).toContain('data assets');

            // Test structured property warnings
            const structuredPropertyWarnings = getValidationWarnings(
                [EntityType.GlossaryTerm, EntityType.Domain],
                ['structuredProperties.urn:li:structuredProperty:my_custom_property'],
                [],
            );
            expect(structuredPropertyWarnings).toHaveLength(1);
            expect(structuredPropertyWarnings[0].message).toContain('Structured Property');
            expect(structuredPropertyWarnings[0].message).toContain('glossary terms, domains');
            expect(structuredPropertyWarnings[0].message).toContain('data assets');

            // Test placeholder warnings
            const placeholderWarnings = getValidationWarnings(
                [EntityType.GlossaryTerm],
                ['__structuredPropertyRef', '__ownershipTypeRef'],
                [],
            );
            expect(placeholderWarnings).toHaveLength(2);
            expect(placeholderWarnings[0].message).toContain('Structured Property');
            expect(placeholderWarnings[1].message).toContain('Ownership Type');
        });
    });

    // =============================================================================
    // DATA CONTRACT AND LINEAGE METRICS (BACKEND-VERIFIED)
    // =============================================================================

    describe('Data contract and lineage metrics (backend-verified)', () => {
        it('should support upstream lineage for datasets only', () => {
            // Upstream lineage assets (always available)
            expect(isPropertySupportedForEntities('upstreamLineage.upstreams', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('upstreamLineage.upstreams', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('upstreamLineage.upstreams', [EntityType.Dashboard])).toBe(false);
        });

        it('should support data contract features for datasets only', () => {
            // Contract state (backend-available)
            expect(isPropertySupportedForEntities('dataContractStatus.state', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('dataContractStatus.state', [EntityType.Chart])).toBe(false);
            expect(isPropertySupportedForEntities('dataContractStatus.state', [EntityType.Dashboard])).toBe(false);
        });

        it('should generate warnings for lineage/contract metrics on non-datasets', () => {
            // Test lineage metrics warnings
            const lineageWarnings = getValidationWarnings(
                [EntityType.Chart, EntityType.Dashboard, EntityType.GlossaryTerm],
                ['upstreamLineage.upstreams'],
                [],
            );
            expect(lineageWarnings).toHaveLength(1);
            expect(lineageWarnings[0].message).toContain('upstreamLineage.upstreams');
            expect(lineageWarnings[0].message).toContain('charts, dashboards, glossary terms');

            // Test data contract warnings
            const contractWarnings = getValidationWarnings(
                [EntityType.Chart, EntityType.Dashboard],
                ['dataContractFeatures.hasDataContract', 'dataContractFeatures.contractState'],
                [],
            );
            expect(contractWarnings).toHaveLength(2);
            expect(contractWarnings[0].message).toContain('hasDataContract');
            expect(contractWarnings[0].message).toContain('charts, dashboards');
        });

        it('should validate complex test definitions with lineage and contract properties', () => {
            const testDefinition: TestDefinition = {
                on: {
                    types: ['DATASET'],
                    conditions: [
                        { property: 'upstreamLineage.upstreams', operator: 'exists' },
                        { property: 'dataContractStatus.state', operator: 'equal_to', values: ['ACTIVE'] },
                    ],
                },
                rules: [{ property: 'dataContractStatus.state', operator: 'equal_to', values: ['ACTIVE'] }],
                actions: {
                    passing: [{ type: ActionId.ADD_OWNERS, values: [] }],
                    failing: [{ type: ActionId.ADD_TAGS, values: [] }],
                },
            };

            const warnings = validateCompleteTestDefinition([EntityType.Dataset], testDefinition);
            expect(warnings).toHaveLength(0); // Should be valid for datasets
        });

        it('should generate warnings for mixed entity types with dataset-specific metrics', () => {
            const testDefinition: TestDefinition = {
                on: {
                    types: ['DATASET', 'CHART'],
                    conditions: [{ property: 'upstreamLineage.upstreams', operator: 'exists' }],
                },
                rules: [{ property: 'dataContractStatus.state', operator: 'equal_to', values: ['ACTIVE'] }],
                actions: {
                    passing: [],
                    failing: [],
                },
            };

            const warnings = validateCompleteTestDefinition([EntityType.Dataset, EntityType.Chart], testDefinition);
            expect(warnings).toHaveLength(2); // Both properties should generate warnings for Chart
            expect(warnings[0].message).toContain('charts');
            expect(warnings[1].message).toContain('charts');
        });

        it('should validate test definitions with schema field structured properties', () => {
            const testDefinition: TestDefinition = {
                on: {
                    types: [EntityType.Dataset],
                },
                rules: {
                    and: [
                        {
                            property: SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
                            operator: 'EQUAL_TO',
                            values: ['sensitive'],
                        },
                        {
                            property:
                                'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime',
                            operator: 'GREATER_THAN',
                            values: ['365'],
                        },
                    ],
                },
                actions: {
                    passing: [],
                    failing: [],
                },
            };

            // Should be valid for datasets
            const warnings1 = validateCompleteTestDefinition([EntityType.Dataset], testDefinition);
            expect(warnings1).toHaveLength(0);

            // Should generate warnings for non-dataset entities
            const warnings2 = validateCompleteTestDefinition([EntityType.Chart, EntityType.Dashboard], testDefinition);
            expect(warnings2.length).toBeGreaterThan(0);
            expect(warnings2.some((w) => w.message.includes('Column Structured Property'))).toBe(true);
        });

        it('should handle structured properties with different entity type combinations', async () => {
            // Import enhanced validation functions
            const { getEnhancedValidationWarnings, isStructuredPropertySupportedForEntities } = await import(
                '@app/tests/builder/validation/structuredPropertyValidation'
            );

            const mockCache = {
                // Scenario 1: Dataset only
                'urn:li:structuredProperty:dataset_only': {
                    entityTypes: [EntityType.Dataset],
                    valueType: 'string',
                    displayName: 'Dataset Only Property',
                },
                // Scenario 2: Multiple entity types (datasets + domains + charts)
                'urn:li:structuredProperty:multi_entity': {
                    entityTypes: [EntityType.Dataset, EntityType.Domain, EntityType.Chart],
                    valueType: 'string',
                    displayName: 'Multi Entity Property',
                },
                // Scenario 3: No restrictions (applies to all data assets)
                'urn:li:structuredProperty:unrestricted': {
                    entityTypes: [], // Empty means all data assets
                    valueType: 'boolean',
                    displayName: 'Unrestricted Property',
                },
            };

            // Test Dataset-only property
            expect(
                isStructuredPropertySupportedForEntities(
                    'structuredProperties.urn:li:structuredProperty:dataset_only',
                    [EntityType.Dataset],
                    mockCache,
                ),
            ).toBe(true);

            expect(
                isStructuredPropertySupportedForEntities(
                    'structuredProperties.urn:li:structuredProperty:dataset_only',
                    [EntityType.Chart, EntityType.Domain],
                    mockCache,
                ),
            ).toBe(false);

            // Test Multi-entity property
            expect(
                isStructuredPropertySupportedForEntities(
                    'structuredProperties.urn:li:structuredProperty:multi_entity',
                    [EntityType.Dataset, EntityType.Domain],
                    mockCache,
                ),
            ).toBe(true);

            expect(
                isStructuredPropertySupportedForEntities(
                    'structuredProperties.urn:li:structuredProperty:multi_entity',
                    [EntityType.Dataset, EntityType.GlossaryTerm], // GlossaryTerm not supported
                    mockCache,
                ),
            ).toBe(false);

            // Test validation warnings show helpful messages
            const warnings = getEnhancedValidationWarnings(
                [EntityType.Chart, EntityType.GlossaryTerm],
                ['structuredProperties.urn:li:structuredProperty:dataset_only'],
                [],
                mockCache,
            );

            expect(warnings).toHaveLength(1);
            expect(warnings[0].message).toContain('Dataset Only Property');
            expect(warnings[0].message).toContain('only works with: dataset');
        });
    });
});
