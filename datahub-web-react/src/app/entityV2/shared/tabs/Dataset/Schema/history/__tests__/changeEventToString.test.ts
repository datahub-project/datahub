import {
    CATEGORY_APPLICATION,
    CATEGORY_DOMAIN,
    CATEGORY_STRUCTURED_PROPERTY,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import {
    getChangeEventString,
    getDocumentationString,
    stripEntityUrns,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/changeEventToString';
import { ChangeCategoryType, ChangeEvent, ChangeOperationType } from '@src/types.generated';

describe('getChangeEventString', () => {
    describe('Technical Schema Changes', () => {
        it('should handle adding a column with specified field path', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Add,
                parameters: [{ key: 'fieldPath', value: 'test.field.path' }],
                description: 'Original description',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added column test.field.path.');
        });

        it('should handle removing a column', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Remove,
                parameters: [
                    {
                        key: 'fieldPath',
                        value: '[version=2.0].[type=struct].[type=array].[type=struct].addresses.[type=string].zip',
                    },
                ],
                description: 'Original description',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed column addresses.zip.');
        });

        it('should handle v2 field path column', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Remove,
                parameters: [{ key: 'fieldPath', value: 'test.field.path' }],
                description: 'Original description',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed column test.field.path.');
        });

        it('should handle modifying a column (description or type change)', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Modify,
                parameters: [{ key: 'fieldPath', value: 'user.email' }],
                description: "The description for 'user.email' has been changed.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Modified column user.email.');
        });

        it('should strip v2 field path for modified columns', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Modify,
                parameters: [
                    {
                        key: 'fieldPath',
                        value: '[version=2.0].[type=struct].[type=string].address.city',
                    },
                ],
                description: "The description for 'address.city' has been changed.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Modified column address.city.');
        });
    });

    describe('Documentation Changes', () => {
        it('should handle empty asset documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                parameters: [{ key: 'description', value: '' }],
                description: 'Original description',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Asset documentation is empty.');
        });

        it('should handle setting asset documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                parameters: [{ key: 'description', value: 'New asset description' }],
                description: 'Original description',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Set asset documentation to New asset description');
        });

        it('should handle setting field documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                modifier: 'test.field',
                parameters: [{ key: 'description', value: 'New field description' }],
                description: 'Original description',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Set field documentation for test.field to New field description');
        });

        it('should handle setting v2 field documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                modifier: '[version=2.0].[type=struct].[type=array].[type=struct].addresses.[type=string].zip',
                parameters: [{ key: 'description', value: 'New field description' }],
                description: 'Original description',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Set field documentation for addresses.zip to New field description');
        });

        it('should show column name for SchemaMetadata field description changes (no description param)', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                modifier: '[version=2.0].[type=struct].[type=string].address.street',
                description:
                    "The description 'Street address' for the field 'address.street' of 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)' has been added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toContain('address.street');
            expect(result).toMatch(/^\[address\.street\]/);
        });

        it('should show simple column name for SchemaMetadata field description changes', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                modifier: 'user_email',
                description: "The description 'User email address' for the field 'user_email' has been added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe(
                "[user_email] The description 'User email address' for the field 'user_email' has been added.",
            );
        });
    });

    describe('Tag Changes', () => {
        it('should handle adding an entity-level tag', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Tag,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:tag:PII',
                parameters: [
                    { key: 'tagUrn', value: 'urn:li:tag:PII' },
                    { key: 'context', value: '{}' },
                ],
                description: "Tag 'PII' added to entity.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added tag "PII".');
        });

        it('should handle removing an entity-level tag', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Tag,
                operation: ChangeOperationType.Remove,
                modifier: 'urn:li:tag:PII',
                parameters: [
                    { key: 'tagUrn', value: 'urn:li:tag:PII' },
                    { key: 'context', value: '{}' },
                ],
                description: "Tag 'PII' removed from entity.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed tag "PII".');
        });

        it('should handle adding a field-level tag', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Tag,
                operation: ChangeOperationType.Add,
                parameters: [
                    { key: 'tagUrn', value: 'urn:li:tag:Sensitive' },
                    { key: 'fieldPath', value: 'email_address' },
                ],
                description: "Tag 'Sensitive' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added tag "Sensitive" to field email_address.');
        });

        it('should fall back to description for unknown operations', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Tag,
                operation: ChangeOperationType.Modify,
                parameters: [{ key: 'tagUrn', value: 'urn:li:tag:Updated' }],
                description: 'Tag was modified.',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Tag was modified.');
        });
    });

    describe('Glossary Term Changes', () => {
        it('should handle adding an entity-level glossary term', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:glossaryTerm:customer_id',
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:customer_id' },
                    { key: 'context', value: '{}' },
                ],
                description: "Term 'customer_id' added to entity.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added term "customer_id".');
        });

        it('should handle removing an entity-level glossary term', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Remove,
                modifier: 'urn:li:glossaryTerm:revenue',
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:revenue' },
                    { key: 'context', value: '{}' },
                ],
                description: "Term 'revenue' removed from entity.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed term "revenue".');
        });

        it('should handle adding a field-level glossary term', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Add,
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:email' },
                    { key: 'fieldPath', value: 'user.email' },
                ],
                description: "Term 'email' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added term "email" to field user.email.');
        });

        it('should render "Is A" as inherited term', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:li:glossaryTerm:TestTerm',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:glossaryTerm:AccountBalance',
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:AccountBalance' },
                    { key: 'relationshipType', value: 'Is A' },
                ],
                description: "'Is A' relationship 'AccountBalance' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added inherited term "AccountBalance".');
        });

        it('should render "Has A" as contained term', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:li:glossaryTerm:TestTerm',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Remove,
                modifier: 'urn:li:glossaryTerm:Revenue',
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:Revenue' },
                    { key: 'relationshipType', value: 'Has A' },
                ],
                description: "'Has A' relationship 'Revenue' removed.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed contained term "Revenue".');
        });

        it('should render "Has Value" as value term', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:li:glossaryTerm:ColorEnum',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:glossaryTerm:RED',
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:RED' },
                    { key: 'relationshipType', value: 'Has Value' },
                ],
                description: "'Has Value' relationship 'RED' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added value term "RED".');
        });

        it('should render "Is Related To" as related term', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:li:glossaryTerm:TestTerm',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:glossaryTerm:DataQuality',
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:DataQuality' },
                    { key: 'relationshipType', value: 'Is Related To' },
                ],
                description: "'Is Related To' relationship 'DataQuality' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added related term "DataQuality".');
        });

        it('should safely handle unknown relationship types as "related"', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:li:glossaryTerm:TestTerm',
                category: ChangeCategoryType.GlossaryTerm,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:glossaryTerm:SomeTerm',
                parameters: [
                    { key: 'termUrn', value: 'urn:li:glossaryTerm:SomeTerm' },
                    { key: 'relationshipType', value: 'SomeNewRelationship' },
                ],
                description: 'Unknown relationship added.',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added related term "SomeTerm".');
        });
    });

    describe('Ownership Changes', () => {
        it('should handle adding an owner with type', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Ownership,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:corpuser:jane_doe',
                parameters: [
                    { key: 'ownerUrn', value: 'urn:li:corpuser:jane_doe' },
                    { key: 'ownerType', value: 'TECHNICAL_OWNER' },
                ],
                description: "'jane_doe' added as a `TECHNICAL_OWNER` of 'urn:test'.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added owner "jane_doe" (Technical Owner).');
        });

        it('should handle removing an owner with type', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Ownership,
                operation: ChangeOperationType.Remove,
                modifier: 'urn:li:corpuser:bob_smith',
                parameters: [
                    { key: 'ownerUrn', value: 'urn:li:corpuser:bob_smith' },
                    { key: 'ownerType', value: 'DATA_STEWARD' },
                ],
                description: "'bob_smith' removed as a `DATA_STEWARD` of 'urn:test'.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed owner "bob_smith" (Data Steward).');
        });

        it('should prefer ownerTypeUrn over ownerType enum', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Ownership,
                operation: ChangeOperationType.Add,
                parameters: [
                    { key: 'ownerUrn', value: 'urn:li:corpuser:jane_doe' },
                    { key: 'ownerType', value: 'CUSTOM' },
                    { key: 'ownerTypeUrn', value: 'urn:li:ownershipType:__system__business_owner' },
                ],
                description: "'jane_doe' added as owner.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added owner "jane_doe" (Business Owner).');
        });

        it('should suppress NONE owner type', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Ownership,
                operation: ChangeOperationType.Add,
                parameters: [
                    { key: 'ownerUrn', value: 'urn:li:corpuser:admin' },
                    { key: 'ownerType', value: 'NONE' },
                ],
                description: "'admin' added as owner.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added owner "admin".');
        });

        it('should suppress CUSTOM owner type when no typeUrn present', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Ownership,
                operation: ChangeOperationType.Add,
                parameters: [
                    { key: 'ownerUrn', value: 'urn:li:corpuser:admin' },
                    { key: 'ownerType', value: 'CUSTOM' },
                ],
                description: "'admin' added as owner.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added owner "admin".');
        });

        it('should handle owner without type parameter', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Ownership,
                operation: ChangeOperationType.Add,
                parameters: [{ key: 'ownerUrn', value: 'urn:li:corpuser:admin' }],
                description: "'admin' added as owner.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added owner "admin".');
        });

        it('should fall back to description for unknown operations', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Ownership,
                operation: ChangeOperationType.Modify,
                parameters: [{ key: 'ownerUrn', value: 'urn:li:corpuser:admin' }],
                description: 'Ownership was modified.',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Ownership was modified.');
        });
    });

    describe('Domain Changes', () => {
        it('should handle adding a domain', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_DOMAIN as ChangeCategoryType,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:domain:engineering',
                parameters: [{ key: 'domainUrn', value: 'urn:li:domain:engineering' }],
                description: "Domain 'engineering' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added to domain "engineering".');
        });

        it('should handle removing a domain', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_DOMAIN as ChangeCategoryType,
                operation: ChangeOperationType.Remove,
                modifier: 'urn:li:domain:marketing',
                parameters: [{ key: 'domainUrn', value: 'urn:li:domain:marketing' }],
                description: "Domain 'marketing' removed.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed from domain "marketing".');
        });

        it('should fall back to description for unknown operations', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_DOMAIN as ChangeCategoryType,
                operation: ChangeOperationType.Modify,
                parameters: [{ key: 'domainUrn', value: 'urn:li:domain:test' }],
                description: 'Domain was modified.',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Domain was modified.');
        });
    });

    describe('Structured Property Changes', () => {
        it('should handle adding a structured property', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_STRUCTURED_PROPERTY as ChangeCategoryType,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:structuredProperty:data_governance.retention_period',
                parameters: [
                    { key: 'propertyUrn', value: 'urn:li:structuredProperty:data_governance.retention_period' },
                    { key: 'propertyValues', value: '["90 days"]' },
                ],
                description: "Structured property 'retention_period' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Set structured property "data_governance.retention_period" to "90 days".');
        });

        it('should handle removing a structured property', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_STRUCTURED_PROPERTY as ChangeCategoryType,
                operation: ChangeOperationType.Remove,
                modifier: 'urn:li:structuredProperty:classification',
                parameters: [
                    { key: 'propertyUrn', value: 'urn:li:structuredProperty:classification' },
                    { key: 'propertyValues', value: '["internal"]' },
                ],
                description: "Structured property 'classification' removed.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed structured property "classification".');
        });

        it('should handle modifying a structured property', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_STRUCTURED_PROPERTY as ChangeCategoryType,
                operation: ChangeOperationType.Modify,
                modifier: 'urn:li:structuredProperty:priority',
                parameters: [
                    { key: 'propertyUrn', value: 'urn:li:structuredProperty:priority' },
                    { key: 'propertyValues', value: '["high"]' },
                ],
                description: "Structured property 'priority' modified.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Updated structured property "priority" to "high".');
        });

        it('should handle multiple values', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_STRUCTURED_PROPERTY as ChangeCategoryType,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:structuredProperty:tags',
                parameters: [
                    { key: 'propertyUrn', value: 'urn:li:structuredProperty:tags' },
                    { key: 'propertyValues', value: '["alpha","beta"]' },
                ],
                description: "Structured property 'tags' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Set structured property "tags" to "alpha", "beta".');
        });
    });

    describe('Application Changes', () => {
        it('should handle adding an application', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_APPLICATION as ChangeCategoryType,
                operation: ChangeOperationType.Add,
                modifier: 'urn:li:application:data-platform',
                description: "Application 'data-platform' added.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Added to application "data-platform".');
        });

        it('should handle removing an application', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_APPLICATION as ChangeCategoryType,
                operation: ChangeOperationType.Remove,
                modifier: 'urn:li:application:analytics',
                description: "Application 'analytics' removed.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Removed from application "analytics".');
        });

        it('should fall back to description for unknown operations', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: CATEGORY_APPLICATION as ChangeCategoryType,
                operation: ChangeOperationType.Modify,
                modifier: 'urn:li:application:test',
                description: 'Application was modified.',
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe('Application was modified.');
        });
    });

    describe('Entity URN stripping', () => {
        it('should strip entity URN from documentation fallthrough descriptions', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:li:glossaryTerm:AccountBalance',
                category: ChangeCategoryType.Documentation,
                operation: ChangeOperationType.Add,
                description:
                    "Documentation for 'urn:li:glossaryTerm:AccountBalance' has been added: 'amount of money'.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe("Documentation has been added: 'amount of money'.");
        });

        it('should strip entity URN from domain name change descriptions', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:li:domain:engineering',
                category: ChangeCategoryType.Documentation,
                operation: ChangeOperationType.Modify,
                description: "Name of 'urn:li:domain:engineering' has been changed from 'Eng' to 'Engineering'.",
            };

            const result = getChangeEventString(changeEvent);
            expect(result).toBe("Name has been changed from 'Eng' to 'Engineering'.");
        });

        it('should strip "to entity" URN pattern from backend descriptions', () => {
            const result = stripEntityUrns("Term 'revenue' added to entity 'urn:li:dataset:foo'.");
            expect(result).toBe("Term 'revenue' added.");
        });

        it('should handle descriptions without URNs unchanged', () => {
            const result = stripEntityUrns('Added owner "jane".');
            expect(result).toBe('Added owner "jane".');
        });

        it('should handle null/undefined safely', () => {
            expect(stripEntityUrns(null)).toBe('');
            expect(stripEntityUrns(undefined)).toBe('');
        });
    });

    describe('Backward compatibility', () => {
        it('getDocumentationString should still work as an alias', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Add,
                parameters: [{ key: 'fieldPath', value: 'test_column' }],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Added column test_column.');
        });
    });
});
