import { ActionId } from '@app/tests/builder/steps/definition/builder/property/types/action';
import {
    LogicalOperatorType,
    LogicalPredicate,
    PropertyPredicate,
} from '@app/tests/builder/steps/definition/builder/types';
import {
    getPropertiesFromPredicate,
    getValidationWarnings,
    isActionSupportedForEntities,
    isPropertySupportedForEntities,
} from '@app/tests/builder/validation/utils';

import { EntityType } from '@types';

describe('Validation Utils', () => {
    describe('isPropertySupportedForEntities', () => {
        it('should support universal properties for all entities', () => {
            // ownership.owners.owner is a universal property
            expect(isPropertySupportedForEntities('ownership.owners.owner', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('ownership.owners.owner', [EntityType.GlossaryTerm])).toBe(true);
            expect(
                isPropertySupportedForEntities('ownership.owners.owner', [EntityType.Dataset, EntityType.GlossaryTerm]),
            ).toBe(true);
        });

        it('should not support data entity specific properties for metadata entities', () => {
            // datasetProperties.description is specific to datasets
            expect(isPropertySupportedForEntities('datasetProperties.description', [EntityType.Dataset])).toBe(true);
            expect(isPropertySupportedForEntities('datasetProperties.description', [EntityType.GlossaryTerm])).toBe(
                false,
            );
            expect(
                isPropertySupportedForEntities('datasetProperties.description', [
                    EntityType.Dataset,
                    EntityType.GlossaryTerm,
                ]),
            ).toBe(false);
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

    describe('getPropertiesFromPredicate', () => {
        it('should extract properties from a simple property predicate', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                property: 'datasetProperties.description',
                operator: 'exists',
            };

            expect(getPropertiesFromPredicate(predicate)).toEqual(['datasetProperties.description']);
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

            const properties = getPropertiesFromPredicate(predicate);
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

            const properties = getPropertiesFromPredicate(predicate);
            expect(properties).toHaveLength(2); // Should deduplicate
            expect(properties).toContain('datasetProperties.description');
            expect(properties).toContain('ownership.owners.owner');
        });

        it('should return empty array for null predicate', () => {
            expect(getPropertiesFromPredicate(null)).toEqual([]);
        });

        it('should handle property predicates without property field', () => {
            const predicate: PropertyPredicate = {
                type: 'property',
                operator: 'exists',
            };

            expect(getPropertiesFromPredicate(predicate)).toEqual([]);
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
                [{ type: ActionId.ADD_TAGS }],
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
                [{ type: ActionId.ADD_TAGS }],
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
                [{ type: ActionId.ADD_TAGS }],
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
                [{ type: ActionId.ADD_TAGS }],
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
});
