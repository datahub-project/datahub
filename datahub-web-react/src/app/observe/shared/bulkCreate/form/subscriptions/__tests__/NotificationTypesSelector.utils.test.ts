import { describe, expect, it } from 'vitest';

import { generateSummaryText } from '@app/observe/shared/bulkCreate/form/subscriptions/NotificationTypesSelector.utils';

import { EntityChangeType } from '@types';

describe('generateSummaryText', () => {
    describe('no selections', () => {
        it('should return no notification types selected when no keys are checked', () => {
            const result = generateSummaryText([]);
            expect(result).toBe('No notification types selected');
        });
    });

    describe('single category selections', () => {
        it('should handle assertion failures only', () => {
            const result = generateSummaryText([EntityChangeType.AssertionFailed]);
            expect(result).toBe('Assertion failure notifications enabled');
        });

        it('should handle assertion errors only', () => {
            const result = generateSummaryText([EntityChangeType.AssertionError]);
            expect(result).toBe('Assertion failure notifications enabled');
        });

        it('should handle assertion runs when passed is included', () => {
            const result = generateSummaryText([EntityChangeType.AssertionPassed]);
            expect(result).toBe('Assertion run notifications enabled');
        });

        it('should handle incident updates', () => {
            const result = generateSummaryText([EntityChangeType.IncidentRaised]);
            expect(result).toBe('Incident update notifications enabled');
        });

        it('should handle schema changes', () => {
            const result = generateSummaryText([EntityChangeType.OperationColumnAdded]);
            expect(result).toBe('Schema change notifications enabled');
        });

        it('should handle data operations', () => {
            const result = generateSummaryText([EntityChangeType.OperationRowsInserted]);
            expect(result).toBe('Data operation notifications enabled');
        });

        it('should handle test results', () => {
            const result = generateSummaryText([EntityChangeType.TestFailed]);
            expect(result).toBe('Test result notifications enabled');
        });

        it('should handle deprecation updates', () => {
            const result = generateSummaryText([EntityChangeType.Deprecated]);
            expect(result).toBe('Deprecation update notifications enabled');
        });

        it('should handle ingestion updates', () => {
            const result = generateSummaryText([EntityChangeType.IngestionFailed]);
            expect(result).toBe('Ingestion update notifications enabled');
        });

        it('should handle documentation changes', () => {
            const result = generateSummaryText([EntityChangeType.DocumentationChange]);
            expect(result).toBe('Documentation change notifications enabled');
        });

        it('should handle ownership changes', () => {
            const result = generateSummaryText([EntityChangeType.OwnerAdded]);
            expect(result).toBe('Ownership change notifications enabled');
        });

        it('should handle glossary term changes', () => {
            const result = generateSummaryText([EntityChangeType.GlossaryTermAdded]);
            expect(result).toBe('Glossary term change notifications enabled');
        });

        it('should handle tag changes', () => {
            const result = generateSummaryText([EntityChangeType.TagAdded]);
            expect(result).toBe('Tag change notifications enabled');
        });
    });

    describe('assertion special logic', () => {
        it('should show assertion failures when only failed and error are selected', () => {
            const result = generateSummaryText([EntityChangeType.AssertionFailed, EntityChangeType.AssertionError]);
            expect(result).toBe('Assertion failure notifications enabled');
        });

        it('should show assertion runs when passed is included with failed', () => {
            const result = generateSummaryText([EntityChangeType.AssertionFailed, EntityChangeType.AssertionPassed]);
            expect(result).toBe('Assertion run notifications enabled');
        });

        it('should show assertion runs when passed is included with error', () => {
            const result = generateSummaryText([EntityChangeType.AssertionError, EntityChangeType.AssertionPassed]);
            expect(result).toBe('Assertion run notifications enabled');
        });

        it('should show assertion runs when all assertion types are selected', () => {
            const result = generateSummaryText([
                EntityChangeType.AssertionFailed,
                EntityChangeType.AssertionError,
                EntityChangeType.AssertionPassed,
            ]);
            expect(result).toBe('Assertion run notifications enabled');
        });
    });

    describe('multiple category selections', () => {
        it('should handle two categories', () => {
            const result = generateSummaryText([EntityChangeType.AssertionFailed, EntityChangeType.IncidentRaised]);
            expect(result).toBe('Assertion failure and Incident update notifications enabled');
        });

        it('should handle three categories', () => {
            const result = generateSummaryText([
                EntityChangeType.AssertionFailed,
                EntityChangeType.IncidentRaised,
                EntityChangeType.OperationColumnAdded,
            ]);
            expect(result).toBe('Assertion failure, Incident update, and Schema change notifications enabled');
        });

        it('should handle four categories', () => {
            const result = generateSummaryText([
                EntityChangeType.AssertionFailed,
                EntityChangeType.IncidentRaised,
                EntityChangeType.OperationColumnAdded,
                EntityChangeType.TestFailed,
            ]);
            expect(result).toBe(
                'Assertion failure, Incident update, Schema change, and Test result notifications enabled',
            );
        });

        it('should handle many categories', () => {
            const result = generateSummaryText([
                EntityChangeType.AssertionFailed,
                EntityChangeType.IncidentRaised,
                EntityChangeType.OperationColumnAdded,
                EntityChangeType.TestFailed,
                EntityChangeType.Deprecated,
                EntityChangeType.OwnerAdded,
            ]);
            expect(result).toBe(
                'Assertion failure, Incident update, Schema change, Test result, Deprecation update, and Ownership change notifications enabled',
            );
        });
    });

    describe('comprehensive category coverage', () => {
        it('should handle multiple assertion types in each category', () => {
            // Schema changes - multiple types
            let result = generateSummaryText([
                EntityChangeType.OperationColumnAdded,
                EntityChangeType.OperationColumnRemoved,
                EntityChangeType.OperationColumnModified,
            ]);
            expect(result).toBe('Schema change notifications enabled');

            // Incident updates - multiple types
            result = generateSummaryText([EntityChangeType.IncidentRaised, EntityChangeType.IncidentResolved]);
            expect(result).toBe('Incident update notifications enabled');

            // Data operations - multiple types
            result = generateSummaryText([
                EntityChangeType.OperationRowsInserted,
                EntityChangeType.OperationRowsRemoved,
                EntityChangeType.OperationRowsUpdated,
            ]);
            expect(result).toBe('Data operation notifications enabled');

            // Test results - multiple types
            result = generateSummaryText([EntityChangeType.TestFailed, EntityChangeType.TestPassed]);
            expect(result).toBe('Test result notifications enabled');

            // Deprecation updates - multiple types
            result = generateSummaryText([EntityChangeType.Deprecated, EntityChangeType.Undeprecated]);
            expect(result).toBe('Deprecation update notifications enabled');

            // Ingestion updates - multiple types
            result = generateSummaryText([EntityChangeType.IngestionFailed, EntityChangeType.IngestionSucceeded]);
            expect(result).toBe('Ingestion update notifications enabled');

            // Ownership changes - multiple types
            result = generateSummaryText([EntityChangeType.OwnerAdded, EntityChangeType.OwnerRemoved]);
            expect(result).toBe('Ownership change notifications enabled');

            // Glossary term changes - multiple types
            result = generateSummaryText([
                EntityChangeType.GlossaryTermAdded,
                EntityChangeType.GlossaryTermRemoved,
                EntityChangeType.GlossaryTermProposed,
            ]);
            expect(result).toBe('Glossary term change notifications enabled');

            // Tag changes - multiple types
            result = generateSummaryText([
                EntityChangeType.TagAdded,
                EntityChangeType.TagRemoved,
                EntityChangeType.TagProposed,
            ]);
            expect(result).toBe('Tag change notifications enabled');
        });
    });

    describe('edge cases and complex combinations', () => {
        it('should prioritize assertion runs when mixed with other categories', () => {
            const result = generateSummaryText([
                EntityChangeType.AssertionFailed,
                EntityChangeType.AssertionPassed,
                EntityChangeType.IncidentRaised,
            ]);
            expect(result).toBe('Assertion run and Incident update notifications enabled');
        });

        it('should handle all possible EntityChangeType values', () => {
            const allTypes = [
                EntityChangeType.AssertionError,
                EntityChangeType.AssertionFailed,
                EntityChangeType.AssertionPassed,
                EntityChangeType.Deprecated,
                EntityChangeType.DocumentationChange,
                EntityChangeType.GlossaryTermAdded,
                EntityChangeType.GlossaryTermProposed,
                EntityChangeType.GlossaryTermRemoved,
                EntityChangeType.IncidentRaised,
                EntityChangeType.IncidentResolved,
                EntityChangeType.IngestionFailed,
                EntityChangeType.IngestionSucceeded,
                EntityChangeType.OperationColumnAdded,
                EntityChangeType.OperationColumnModified,
                EntityChangeType.OperationColumnRemoved,
                EntityChangeType.OperationRowsInserted,
                EntityChangeType.OperationRowsRemoved,
                EntityChangeType.OperationRowsUpdated,
                EntityChangeType.OwnerAdded,
                EntityChangeType.OwnerRemoved,
                EntityChangeType.TagAdded,
                EntityChangeType.TagProposed,
                EntityChangeType.TagRemoved,
                EntityChangeType.TestFailed,
                EntityChangeType.TestPassed,
                EntityChangeType.Undeprecated,
            ];

            const result = generateSummaryText(allTypes);
            expect(result).toContain('Assertion run'); // Should show runs since passed is included
            expect(result).toContain('and'); // Should have multiple categories
            expect(result).not.toBe('No notification types selected');
        });
    });
});
