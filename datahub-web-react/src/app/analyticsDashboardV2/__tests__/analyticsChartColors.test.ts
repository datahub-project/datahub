/**
 * Unit tests for Analytics Chart Color Assignment System
 *
 * Tests cover:
 * - Entity color matching
 * - Name variation handling
 * - Fallback to qualitative colors
 * - Generated colors for large series
 * - User override functionality
 */
import { describe, expect, test } from 'vitest';

import { assignAnalyticsChartColors } from '@app/analyticsDashboardV2/utils/analyticsChartColors';
import { DATAHUB_ENTITY_COLORS, QUALITATIVE_COLORS } from '@app/analyticsDashboardV2/utils/chartColorConstants';
import { findDataHubEntityColor, getAllEntityMatches } from '@app/analyticsDashboardV2/utils/chartColorMatcher';

describe('DataHub Entity Color Matching', () => {
    test('assigns entity colors correctly for exact matches', () => {
        const result = assignAnalyticsChartColors(['schema', 'incidents', 'dataset']);

        expect(result.assignments.schema.color).toBe(DATAHUB_ENTITY_COLORS.schema);
        expect(result.assignments.incidents.color).toBe(DATAHUB_ENTITY_COLORS.incidents);
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(result.assignments.schema.strategy.source).toBe('datahub-entity');
    });

    test('handles entity name variations - case insensitive', () => {
        expect(findDataHubEntityColor('DATASET')).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('Dataset')).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('DaTaSeT')).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles entity name variations - separators', () => {
        expect(findDataHubEntityColor('data_set')).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('data-set')).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('data set')).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles entity name variations - compound names', () => {
        expect(findDataHubEntityColor('data_product')).toBe(DATAHUB_ENTITY_COLORS.dataproduct);
        expect(findDataHubEntityColor('DataProduct')).toBe(DATAHUB_ENTITY_COLORS.dataproduct);
        expect(findDataHubEntityColor('DATA_PRODUCT')).toBe(DATAHUB_ENTITY_COLORS.dataproduct);
    });

    test('handles partial matches for entity types', () => {
        // Note: partial matching works when the entity type is contained in the key
        const datasetMatch = findDataHubEntityColor('dataset_view');
        const schemaMatch = findDataHubEntityColor('schema_tab');

        // These should match because 'dataset' is contained in 'dataset_view'
        expect(datasetMatch).not.toBeNull();
        expect(schemaMatch).not.toBeNull();
    });

    test('returns null for unknown entity types', () => {
        expect(findDataHubEntityColor('unknown_type')).toBeNull();
        expect(findDataHubEntityColor('random_entity')).toBeNull();
    });
});

describe('Qualitative Color Fallback', () => {
    test('falls back to qualitative colors for unknown entities', () => {
        const unknownEntities = ['unknownType1', 'unknownType2', 'unknownType3'];
        const result = assignAnalyticsChartColors(unknownEntities);

        expect(result.assignments.unknownType1.strategy.type).toBe('qualitative');
        expect(result.assignments.unknownType1.color).toBe(QUALITATIVE_COLORS[0]);
        expect(result.assignments.unknownType2.color).toBe(QUALITATIVE_COLORS[1]);
        expect(result.assignments.unknownType3.color).toBe(QUALITATIVE_COLORS[2]);
    });

    test('uses qualitative colors after entity colors are exhausted', () => {
        const mixedEntities = ['dataset', 'unknownType1', 'schema', 'unknownType2'];
        const result = assignAnalyticsChartColors(mixedEntities);

        expect(result.assignments.dataset.strategy.source).toBe('datahub-entity');
        expect(result.assignments.schema.strategy.source).toBe('datahub-entity');
        expect(result.assignments.unknownType1.strategy.type).toBe('qualitative');
        expect(result.assignments.unknownType2.strategy.type).toBe('qualitative');
    });

    test('tracks unused qualitative colors', () => {
        const result = assignAnalyticsChartColors(['unknownType1', 'unknownType2']);

        expect(result.unusedColors.length).toBe(QUALITATIVE_COLORS.length - 2);
        expect(result.unusedColors).not.toContain(QUALITATIVE_COLORS[0]);
        expect(result.unusedColors).not.toContain(QUALITATIVE_COLORS[1]);
    });
});

describe('Generated Colors for Large Series', () => {
    test('generates colors for large series beyond qualitative palette', () => {
        const manyEntities = Array.from({ length: 20 }, (_, i) => `entity${i}`);
        const result = assignAnalyticsChartColors(manyEntities);

        expect(Object.keys(result.assignments)).toHaveLength(20);
        expect(result.generatedCount).toBeGreaterThan(0);
    });

    test('generated colors have correct strategy', () => {
        const manyEntities = Array.from({ length: 15 }, (_, i) => `entity${i}`);
        const result = assignAnalyticsChartColors(manyEntities);

        const generatedAssignments = Object.values(result.assignments).filter((a) => a.strategy.type === 'generated');

        expect(generatedAssignments.length).toBe(result.generatedCount);
        generatedAssignments.forEach((assignment) => {
            expect(assignment.strategy.source).toBe('hsl-golden-ratio');
        });
    });

    test('all series get unique colors', () => {
        const manyEntities = Array.from({ length: 25 }, (_, i) => `entity${i}`);
        const result = assignAnalyticsChartColors(manyEntities);

        const colors = Object.values(result.assignments).map((a) => a.color);
        const uniqueColors = new Set(colors);

        expect(uniqueColors.size).toBe(colors.length);
    });
});

describe('User Override Functionality', () => {
    test('respects user overrides', () => {
        const overrides = { schema: '#FF0000', dataset: '#00FF00' };
        const result = assignAnalyticsChartColors(['schema', 'dataset'], overrides);

        expect(result.assignments.schema.color).toBe('#FF0000');
        expect(result.assignments.schema.userOverride).toBe('#FF0000');
        expect(result.assignments.dataset.color).toBe('#00FF00');
        expect(result.assignments.dataset.userOverride).toBe('#00FF00');
    });

    test('user overrides take precedence over entity colors', () => {
        const overrides = { dataset: '#123456' };
        const result = assignAnalyticsChartColors(['dataset'], overrides);

        expect(result.assignments.dataset.color).toBe('#123456');
        expect(result.assignments.dataset.color).not.toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('partial overrides work with automatic assignment', () => {
        const overrides = { entity1: '#AAAAAA' };
        const result = assignAnalyticsChartColors(['entity1', 'dataset', 'entity2'], overrides);

        expect(result.assignments.entity1.color).toBe('#AAAAAA');
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(result.assignments.entity2.strategy.type).toBe('qualitative');
    });
});

describe('Color Assignment Priority', () => {
    test('follows correct priority order', () => {
        const overrides = { override: '#CUSTOM' };
        const seriesKeys = ['override', 'dataset', 'unknown', 'another'];

        const result = assignAnalyticsChartColors(seriesKeys, overrides);

        // Priority 1: User override
        expect(result.assignments.override.strategy.source).toBe('user-override');

        // Priority 2: Entity color
        expect(result.assignments.dataset.strategy.source).toBe('datahub-entity');

        // Priority 3: Qualitative
        expect(result.assignments.unknown.strategy.type).toBe('qualitative');
        expect(result.assignments.another.strategy.type).toBe('qualitative');
    });

    test('does not reuse colors across different sources', () => {
        const seriesKeys = ['dataset', 'schema', 'unknown1', 'unknown2'];
        const result = assignAnalyticsChartColors(seriesKeys);

        const colors = Object.values(result.assignments).map((a) => a.color);
        const uniqueColors = new Set(colors);

        expect(uniqueColors.size).toBe(colors.length);
    });
});

describe('Edge Cases', () => {
    test('handles empty series array', () => {
        const result = assignAnalyticsChartColors([]);

        expect(Object.keys(result.assignments)).toHaveLength(0);
        expect(result.generatedCount).toBe(0);
        expect(result.unusedColors.length).toBe(QUALITATIVE_COLORS.length);
    });

    test('handles single series', () => {
        const result = assignAnalyticsChartColors(['dataset']);

        expect(Object.keys(result.assignments)).toHaveLength(1);
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles duplicate series keys', () => {
        const result = assignAnalyticsChartColors(['dataset', 'dataset']);

        expect(Object.keys(result.assignments)).toHaveLength(1);
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles very long series names', () => {
        const longName = 'a'.repeat(100);
        const result = assignAnalyticsChartColors([longName]);

        expect(result.assignments[longName]).toBeDefined();
        expect(result.assignments[longName].color).toBeTruthy();
    });
});

describe('getAllEntityMatches utility', () => {
    test('returns all matching entities', () => {
        const matches = getAllEntityMatches('dataset');

        expect(matches.length).toBeGreaterThan(0);
        expect(matches.some((m) => m.entity === 'dataset')).toBe(true);
        matches.forEach((match) => {
            expect(match.color).toBeTruthy();
        });
    });

    test('returns empty array for no matches', () => {
        const matches = getAllEntityMatches('xyz123notfound');

        expect(matches.length).toBe(0);
    });
});
