import { Key } from 'react';

import { EntityChangeType } from '@types';

export const generateSummaryText = (checkedKeys: Key[]): string => {
    const categories: string[] = [];

    // Check for assertion-related notifications with special logic
    const hasAssertionFailed = checkedKeys.includes(EntityChangeType.AssertionFailed);
    const hasAssertionError = checkedKeys.includes(EntityChangeType.AssertionError);
    const hasAssertionPassed = checkedKeys.includes(EntityChangeType.AssertionPassed);

    if (hasAssertionFailed || hasAssertionError || hasAssertionPassed) {
        if (hasAssertionPassed) {
            categories.push('Assertion run');
        } else {
            categories.push('Assertion failure');
        }
    }

    // Check for incident-related notifications
    const hasIncidentChanges = checkedKeys.some(
        (key) => key === EntityChangeType.IncidentRaised || key === EntityChangeType.IncidentResolved,
    );
    if (hasIncidentChanges) {
        categories.push('Incident update');
    }

    // Check for schema-related notifications
    const hasSchemaChanges = checkedKeys.some(
        (key) =>
            key === EntityChangeType.OperationColumnAdded ||
            key === EntityChangeType.OperationColumnRemoved ||
            key === EntityChangeType.OperationColumnModified,
    );
    if (hasSchemaChanges) {
        categories.push('Schema change');
    }

    // Check for operational metadata changes
    const hasOperationChanges = checkedKeys.some(
        (key) =>
            key === EntityChangeType.OperationRowsInserted ||
            key === EntityChangeType.OperationRowsRemoved ||
            key === EntityChangeType.OperationRowsUpdated,
    );
    if (hasOperationChanges) {
        categories.push('Data operation');
    }

    // Check for test-related notifications
    const hasTestChanges = checkedKeys.some(
        (key) => key === EntityChangeType.TestFailed || key === EntityChangeType.TestPassed,
    );
    if (hasTestChanges) {
        categories.push('Test result');
    }

    // Check for deprecation status changes
    const hasDeprecationChanges = checkedKeys.some(
        (key) => key === EntityChangeType.Deprecated || key === EntityChangeType.Undeprecated,
    );
    if (hasDeprecationChanges) {
        categories.push('Deprecation update');
    }

    // Check for ingestion status changes
    const hasIngestionChanges = checkedKeys.some(
        (key) => key === EntityChangeType.IngestionFailed || key === EntityChangeType.IngestionSucceeded,
    );
    if (hasIngestionChanges) {
        categories.push('Ingestion update');
    }

    // Check for documentation changes
    const hasDocumentationChanges = checkedKeys.includes(EntityChangeType.DocumentationChange);
    if (hasDocumentationChanges) {
        categories.push('Documentation change');
    }

    // Check for ownership changes
    const hasOwnershipChanges = checkedKeys.some(
        (key) => key === EntityChangeType.OwnerAdded || key === EntityChangeType.OwnerRemoved,
    );
    if (hasOwnershipChanges) {
        categories.push('Ownership change');
    }

    // Check for glossary term changes
    const hasGlossaryChanges = checkedKeys.some(
        (key) =>
            key === EntityChangeType.GlossaryTermAdded ||
            key === EntityChangeType.GlossaryTermRemoved ||
            key === EntityChangeType.GlossaryTermProposed,
    );
    if (hasGlossaryChanges) {
        categories.push('Glossary term change');
    }

    // Check for tag changes
    const hasTagChanges = checkedKeys.some(
        (key) =>
            key === EntityChangeType.TagAdded ||
            key === EntityChangeType.TagRemoved ||
            key === EntityChangeType.TagProposed,
    );
    if (hasTagChanges) {
        categories.push('Tag change');
    }

    if (categories.length === 0) {
        return 'No notification types selected';
    }

    if (categories.length === 1) {
        return `${categories[0]} notifications enabled`;
    }

    if (categories.length === 2) {
        return `${categories[0]} and ${categories[1]} notifications enabled`;
    }

    // For 3 or more categories
    const lastCategory = categories.pop();
    return `${categories.join(', ')}, and ${lastCategory} notifications enabled`;
};
