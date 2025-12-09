import { describe, expect, it } from 'vitest';

import { buildIngestionSourceChatContext } from '@app/ingestV2/source/multiStepBuilder/utils';

describe('buildIngestionSourceChatContext', () => {
    const baseHelpfulContext =
        ' This is a configuration context where the user may ask questions about connection details, authentication, scheduling, or troubleshooting configuration issues.';

    describe('base context', () => {
        it('should build context for creating a new source', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
            });

            expect(result).toBe(`The user is creating a new ingestion source.${baseHelpfulContext}`);
        });

        it('should build context for editing an existing source', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: true,
            });

            expect(result).toBe(`The user is editing an existing ingestion source.${baseHelpfulContext}`);
        });

        it('should include source URN when editing', () => {
            const sourceUrn = 'urn:li:dataSource:123';
            const result = buildIngestionSourceChatContext({
                isEditing: true,
                sourceUrn,
            });

            expect(result).toBe(
                `The user is editing an existing ingestion sourcewith URN: ${sourceUrn}.${baseHelpfulContext}`,
            );
        });

        it('should not include source URN when creating', () => {
            const sourceUrn = 'urn:li:dataSource:123';
            const result = buildIngestionSourceChatContext({
                isEditing: false,
                sourceUrn,
            });

            expect(result).not.toContain('URN');
        });
    });

    describe('source type', () => {
        it('should include source type when provided', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
                sourceType: 'mysql',
            });

            expect(result).toContain('. The source type is "mysql".');
        });

        it('should handle missing source type', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
            });

            expect(result).toContain('The user is creating a new ingestion source.');
            expect(result).not.toContain('source type');
        });
    });

    describe('source name', () => {
        it('should include source name when provided', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
                sourceName: 'Production MySQL',
            });

            expect(result).toContain(' The source name is "Production MySQL".');
        });

        it('should handle missing source name', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
            });

            expect(result).not.toContain('source name');
        });
    });

    describe('current step', () => {
        it('should include current step when provided', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
                currentStep: 'Connection Details',
            });

            expect(result).toContain(' The user is currently on the "Connection Details" step.');
        });

        it('should include step context when both step and context provided', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
                currentStep: 'Connection Details',
                stepContext: 'Configure your database connection settings',
            });

            expect(result).toContain(' The user is currently on the "Connection Details" step.');
            expect(result).toContain(
                'This is the context of what that step is meant for: Configure your database connection settings',
            );
        });

        it('should not include step context when step is missing', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
                stepContext: 'Configure your database connection settings',
            });

            expect(result).not.toContain('This is the context of what that step is meant for');
        });

        it('should handle missing current step', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
            });

            expect(result).not.toContain('currently on the');
        });
    });

    describe('complete scenarios', () => {
        it('should build complete context for creating a new source with all fields', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
                sourceType: 'postgres',
                sourceName: 'Analytics DB',
                currentStep: 'Sync Schedule',
                stepContext: 'Set up your data synchronization schedule',
            });

            expect(result).toBe(
                'The user is creating a new ingestion source. The source type is "postgres". The source name is "Analytics DB". The user is currently on the "Sync Schedule" step. This is the context of what that step is meant for: Set up your data synchronization schedule This is a configuration context where the user may ask questions about connection details, authentication, scheduling, or troubleshooting configuration issues.',
            );
        });

        it('should build complete context for editing an existing source with all fields', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: true,
                sourceUrn: 'urn:li:dataSource:456',
                sourceType: 'snowflake',
                sourceName: 'Data Warehouse',
                currentStep: 'Connection Details',
                stepContext: 'Configure connection parameters',
            });

            expect(result).toBe(
                'The user is editing an existing ingestion sourcewith URN: urn:li:dataSource:456. The source type is "snowflake". The source name is "Data Warehouse". The user is currently on the "Connection Details" step. This is the context of what that step is meant for: Configure connection parameters This is a configuration context where the user may ask questions about connection details, authentication, scheduling, or troubleshooting configuration issues.',
            );
        });

        it('should build minimal context with only required field', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: false,
            });

            expect(result).toBe(`The user is creating a new ingestion source.${baseHelpfulContext}`);
        });

        it('should build context with partial fields', () => {
            const result = buildIngestionSourceChatContext({
                isEditing: true,
                sourceUrn: 'urn:li:dataSource:789',
                sourceName: 'Legacy System',
            });

            expect(result).toBe(
                `The user is editing an existing ingestion sourcewith URN: urn:li:dataSource:789. The source name is "Legacy System".${baseHelpfulContext}`,
            );
        });
    });
});
