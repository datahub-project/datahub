import {
    SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
    SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_REGEX,
    STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
    STRUCTURED_PROPERTY_REFERENCE_REGEX,
} from '@app/tests/builder/steps/definition/builder/property/constants';
import { entityProperties } from '@app/tests/builder/steps/definition/builder/property/types/properties';
import {
    extractSchemaFieldStructuredPropertyReferenceUrn,
    extractStructuredPropertyReferenceUrn,
    getPropertiesForEntityTypes,
    isSchemaFieldStructuredPropertyId,
    isStructuredPropertyId,
} from '@app/tests/builder/steps/definition/builder/property/utils';

import { EntityType } from '@types';

describe('utils', () => {
    describe('getPropertiesForEntityTypes', () => {
        it('test single entity type', () => {
            expect(getPropertiesForEntityTypes([EntityType.Dataset])).toEqual(
                entityProperties.filter((obj) => obj.type === EntityType.Dataset)[0].properties,
            );
        });
        it('test empty entity types', () => {
            expect(getPropertiesForEntityTypes([])).toEqual([]);
        });
        it('test multiple entity type correctly intersects', () => {
            const res = getPropertiesForEntityTypes([EntityType.Dataset, EntityType.Chart, EntityType.Dashboard]);

            // Size of result should be less than both dataset props + chart props.
            expect(res.length).toBeLessThan(
                entityProperties.filter((obj) => obj.type === EntityType.Dataset)[0].properties.length,
            );
            expect(res.length).toBeLessThan(
                entityProperties.filter((obj) => obj.type === EntityType.Chart)[0].properties.length,
            );
            expect(res.length).toBeLessThan(
                entityProperties.filter((obj) => obj.type === EntityType.Dashboard)[0].properties.length,
            );
        });
    });

    describe('Schema Field Structured Property Functions', () => {
        describe('isSchemaFieldStructuredPropertyId', () => {
            it('should return true for schema field structured property placeholder ID', () => {
                expect(
                    isSchemaFieldStructuredPropertyId(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID),
                ).toBe(true);
            });

            it('should return true for schema field structured property with URN', () => {
                const propertyId =
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';
                expect(isSchemaFieldStructuredPropertyId(propertyId)).toBe(true);
            });

            it('should return false for regular structured property', () => {
                const propertyId = 'structuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';
                expect(isSchemaFieldStructuredPropertyId(propertyId)).toBe(false);
            });

            it('should return false for regular property', () => {
                expect(isSchemaFieldStructuredPropertyId('datasetProperties.description')).toBe(false);
            });

            it('should return false for empty string', () => {
                expect(isSchemaFieldStructuredPropertyId('')).toBe(false);
            });

            it('should return false for malformed schema field structured property', () => {
                expect(isSchemaFieldStructuredPropertyId('schemaFieldStructuredProperties.invalid')).toBe(false);
            });
        });

        describe('extractSchemaFieldStructuredPropertyReferenceUrn', () => {
            it('should extract URN from valid schema field structured property ID', () => {
                const propertyId =
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';
                const expectedUrn = 'urn:li:structuredProperty:io.acryl.privacy.retentionTime';
                expect(extractSchemaFieldStructuredPropertyReferenceUrn(propertyId)).toBe(expectedUrn);
            });

            it('should return undefined for placeholder ID', () => {
                expect(
                    extractSchemaFieldStructuredPropertyReferenceUrn(
                        SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
                    ),
                ).toBeUndefined();
            });

            it('should return undefined for regular property', () => {
                expect(
                    extractSchemaFieldStructuredPropertyReferenceUrn('datasetProperties.description'),
                ).toBeUndefined();
            });

            it('should return undefined for regular structured property', () => {
                const propertyId = 'structuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';
                expect(extractSchemaFieldStructuredPropertyReferenceUrn(propertyId)).toBeUndefined();
            });

            it('should return undefined for malformed schema field structured property', () => {
                expect(
                    extractSchemaFieldStructuredPropertyReferenceUrn('schemaFieldStructuredProperties.invalid'),
                ).toBeUndefined();
            });

            it('should handle complex URNs with special characters', () => {
                const propertyId =
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:com.linkedin.common.DatasetProperties';
                const expectedUrn = 'urn:li:structuredProperty:com.linkedin.common.DatasetProperties';
                expect(extractSchemaFieldStructuredPropertyReferenceUrn(propertyId)).toBe(expectedUrn);
            });
        });
    });

    describe('Constants and Regex Patterns', () => {
        describe('SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID', () => {
            it('should have the correct placeholder ID', () => {
                expect(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID).toBe(
                    '__schemaFieldStructuredPropertyRef',
                );
            });

            it('should be different from regular structured property placeholder', () => {
                expect(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID).not.toBe(
                    STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
                );
            });
        });

        describe('SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_REGEX', () => {
            it('should match valid schema field structured property IDs', () => {
                const validIds = [
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime',
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:com.linkedin.common.DatasetProperties',
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:simple',
                ];

                validIds.forEach((id) => {
                    expect(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_REGEX.test(id)).toBe(true);
                });
            });

            it('should not match invalid schema field structured property IDs', () => {
                const invalidIds = [
                    'structuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime', // regular structured property
                    'schemaFieldStructuredProperties.invalid', // missing urn prefix
                    'schemaFieldStructuredProperties.', // empty URN
                    'datasetProperties.description', // regular property
                    SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID, // placeholder
                ];

                invalidIds.forEach((id) => {
                    expect(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_REGEX.test(id)).toBe(false);
                });
            });

            it('should extract URN in capture group', () => {
                const propertyId =
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';
                const match = propertyId.match(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_REGEX);
                expect(match).not.toBeNull();
                expect(match![1]).toBe('urn:li:structuredProperty:io.acryl.privacy.retentionTime');
            });
        });

        describe('Regex Pattern Consistency', () => {
            it('should have consistent regex patterns for both structured property types', () => {
                // Both should follow similar pattern structure
                const regularMatch = 'structuredProperties.urn:li:structuredProperty:test'.match(
                    STRUCTURED_PROPERTY_REFERENCE_REGEX,
                );
                const schemaFieldMatch = 'schemaFieldStructuredProperties.urn:li:structuredProperty:test'.match(
                    SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_REGEX,
                );

                expect(regularMatch).not.toBeNull();
                expect(schemaFieldMatch).not.toBeNull();
                expect(regularMatch![1]).toBe(schemaFieldMatch![1]); // Same URN extracted
            });
        });
    });

    describe('Integration Tests', () => {
        describe('Function Integration', () => {
            it('should work together to identify and extract schema field structured properties', () => {
                const propertyId =
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';

                // Should be identified as schema field structured property
                expect(isSchemaFieldStructuredPropertyId(propertyId)).toBe(true);
                expect(isStructuredPropertyId(propertyId)).toBe(false);

                // Should extract the correct URN
                const extractedUrn = extractSchemaFieldStructuredPropertyReferenceUrn(propertyId);
                expect(extractedUrn).toBe('urn:li:structuredProperty:io.acryl.privacy.retentionTime');

                // Regular structured property extractor should not work
                expect(extractStructuredPropertyReferenceUrn(propertyId)).toBeUndefined();
            });

            it('should distinguish between regular and schema field structured properties', () => {
                const regularPropertyId =
                    'structuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';
                const schemaFieldPropertyId =
                    'schemaFieldStructuredProperties.urn:li:structuredProperty:io.acryl.privacy.retentionTime';

                // Regular structured property
                expect(isStructuredPropertyId(regularPropertyId)).toBe(true);
                expect(isSchemaFieldStructuredPropertyId(regularPropertyId)).toBe(false);
                expect(extractStructuredPropertyReferenceUrn(regularPropertyId)).toBe(
                    'urn:li:structuredProperty:io.acryl.privacy.retentionTime',
                );
                expect(extractSchemaFieldStructuredPropertyReferenceUrn(regularPropertyId)).toBeUndefined();

                // Schema field structured property
                expect(isSchemaFieldStructuredPropertyId(schemaFieldPropertyId)).toBe(true);
                expect(isStructuredPropertyId(schemaFieldPropertyId)).toBe(false);
                expect(extractSchemaFieldStructuredPropertyReferenceUrn(schemaFieldPropertyId)).toBe(
                    'urn:li:structuredProperty:io.acryl.privacy.retentionTime',
                );
                expect(extractStructuredPropertyReferenceUrn(schemaFieldPropertyId)).toBeUndefined();
            });
        });

        describe('Edge Cases', () => {
            it('should handle empty and null inputs gracefully', () => {
                expect(isSchemaFieldStructuredPropertyId('')).toBe(false);
                expect(extractSchemaFieldStructuredPropertyReferenceUrn('')).toBeUndefined();
            });

            it('should handle placeholder IDs correctly', () => {
                expect(
                    isSchemaFieldStructuredPropertyId(SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID),
                ).toBe(true);
                expect(
                    extractSchemaFieldStructuredPropertyReferenceUrn(
                        SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
                    ),
                ).toBeUndefined();

                expect(isStructuredPropertyId(STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID)).toBe(true);
                expect(
                    extractStructuredPropertyReferenceUrn(STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID),
                ).toBeUndefined();
            });

            it('should handle URNs with various formats', () => {
                const testCases = [
                    {
                        input: 'schemaFieldStructuredProperties.urn:li:structuredProperty:simple',
                        expected: 'urn:li:structuredProperty:simple',
                    },
                    {
                        input: 'schemaFieldStructuredProperties.urn:li:structuredProperty:com.company.data.RetentionTime',
                        expected: 'urn:li:structuredProperty:com.company.data.RetentionTime',
                    },
                    {
                        input: 'schemaFieldStructuredProperties.urn:li:structuredProperty:namespace.with.dots.and.CamelCase',
                        expected: 'urn:li:structuredProperty:namespace.with.dots.and.CamelCase',
                    },
                ];

                testCases.forEach(({ input, expected }) => {
                    expect(isSchemaFieldStructuredPropertyId(input)).toBe(true);
                    expect(extractSchemaFieldStructuredPropertyReferenceUrn(input)).toBe(expected);
                });
            });
        });
    });
});
