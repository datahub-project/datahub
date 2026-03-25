// Disabling hardcoded colors rule in test file
/* eslint-disable rulesdir/no-hardcoded-colors */
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
import { DefaultTheme } from 'styled-components';
import { describe, expect, test } from 'vitest';

import { assignAnalyticsChartColors } from '@app/analyticsDashboardV2/utils/analyticsChartColors';
import { QUALITATIVE_COLORS, getDatahubEntityColors } from '@app/analyticsDashboardV2/utils/chartColorConstants';
import { findDataHubEntityColor, getAllEntityMatches } from '@app/analyticsDashboardV2/utils/chartColorMatcher';

const mockTheme = {
    colors: {
        tagsCobaltBlueText: '#303F9F',
        chartsInformationHigh: '#09739A',
        chartsInformationMedium: '#4897B4',
        chartsInformationLow: '#5ABDE1',
        chartsBrandHigh: '#533FD1',
        chartsBrandMedium: '#705EE4',
        chartsBrandLow: '#B0A7EA',
        chartsRedHigh: '#8B1A1A',
        chartsRedMedium: '#D23939',
        iconWarning: '#EE9521',
        textWarning: '#C77100',
        iconSuccess: '#248F5B',
        chartsGreenHigh: '#6CA749',
        chartsGreenMedium: '#92C573',
        chartsGreenLow: '#C0DEAF',
        chartsBlueHigh: '#10737F',
        chartsSeafoamHigh: '#20594B',
        chartsSeafoamMedium: '#80CBC4',
        textTertiary: '#8088A3',
    },
} as unknown as DefaultTheme;

const DATAHUB_ENTITY_COLORS = getDatahubEntityColors(mockTheme);

describe('DataHub Entity Color Matching', () => {
    test('assigns entity colors correctly for exact matches', () => {
        const result = assignAnalyticsChartColors(['schema', 'incidents', 'dataset'], mockTheme);

        expect(result.assignments.schema.color).toBe(DATAHUB_ENTITY_COLORS.schema);
        expect(result.assignments.incidents.color).toBe(DATAHUB_ENTITY_COLORS.incidents);
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(result.assignments.schema.strategy.source).toBe('datahub-entity');
    });

    test('handles entity name variations - case insensitive', () => {
        expect(findDataHubEntityColor('DATASET', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('Dataset', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('DaTaSeT', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles entity name variations - separators', () => {
        expect(findDataHubEntityColor('data_set', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('data-set', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(findDataHubEntityColor('data set', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles entity name variations - compound names', () => {
        expect(findDataHubEntityColor('data_product', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataproduct);
        expect(findDataHubEntityColor('DataProduct', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataproduct);
        expect(findDataHubEntityColor('DATA_PRODUCT', mockTheme)).toBe(DATAHUB_ENTITY_COLORS.dataproduct);
    });

    test('handles partial matches for entity types', () => {
        const datasetMatch = findDataHubEntityColor('dataset_view', mockTheme);
        const schemaMatch = findDataHubEntityColor('schema_tab', mockTheme);

        expect(datasetMatch).not.toBeNull();
        expect(schemaMatch).not.toBeNull();
    });

    test('returns null for unknown entity types', () => {
        expect(findDataHubEntityColor('unknown_type', mockTheme)).toBeNull();
        expect(findDataHubEntityColor('random_entity', mockTheme)).toBeNull();
    });
});

describe('Qualitative Color Fallback', () => {
    test('falls back to qualitative colors for unknown entities', () => {
        const unknownEntities = ['unknownType1', 'unknownType2', 'unknownType3'];
        const result = assignAnalyticsChartColors(unknownEntities, mockTheme);

        expect(result.assignments.unknownType1.strategy.type).toBe('qualitative');
        expect(result.assignments.unknownType1.color).toBe(QUALITATIVE_COLORS[0]);
        expect(result.assignments.unknownType2.color).toBe(QUALITATIVE_COLORS[1]);
        expect(result.assignments.unknownType3.color).toBe(QUALITATIVE_COLORS[2]);
    });

    test('uses qualitative colors after entity colors are exhausted', () => {
        const mixedEntities = ['dataset', 'unknownType1', 'schema', 'unknownType2'];
        const result = assignAnalyticsChartColors(mixedEntities, mockTheme);

        expect(result.assignments.dataset.strategy.source).toBe('datahub-entity');
        expect(result.assignments.schema.strategy.source).toBe('datahub-entity');
        expect(result.assignments.unknownType1.strategy.type).toBe('qualitative');
        expect(result.assignments.unknownType2.strategy.type).toBe('qualitative');
    });

    test('tracks unused qualitative colors', () => {
        const result = assignAnalyticsChartColors(['unknownType1', 'unknownType2'], mockTheme);

        expect(result.unusedColors.length).toBe(QUALITATIVE_COLORS.length - 2);
        expect(result.unusedColors).not.toContain(QUALITATIVE_COLORS[0]);
        expect(result.unusedColors).not.toContain(QUALITATIVE_COLORS[1]);
    });
});

describe('Generated Colors for Large Series', () => {
    test('generates colors for large series beyond qualitative palette', () => {
        const manyEntities = Array.from({ length: 20 }, (_, i) => `entity${i}`);
        const result = assignAnalyticsChartColors(manyEntities, mockTheme);

        expect(Object.keys(result.assignments)).toHaveLength(20);
        expect(result.generatedCount).toBeGreaterThan(0);
    });

    test('generated colors have correct strategy', () => {
        const manyEntities = Array.from({ length: 15 }, (_, i) => `entity${i}`);
        const result = assignAnalyticsChartColors(manyEntities, mockTheme);

        const generatedAssignments = Object.values(result.assignments).filter((a) => a.strategy.type === 'generated');

        expect(generatedAssignments.length).toBe(result.generatedCount);
        generatedAssignments.forEach((assignment) => {
            expect(assignment.strategy.source).toBe('hsl-golden-ratio');
        });
    });

    test('all series get unique colors', () => {
        const manyEntities = Array.from({ length: 25 }, (_, i) => `entity${i}`);
        const result = assignAnalyticsChartColors(manyEntities, mockTheme);

        const colors = Object.values(result.assignments).map((a) => a.color);
        const uniqueColors = new Set(colors);

        expect(uniqueColors.size).toBe(colors.length);
    });
});

describe('User Override Functionality', () => {
    test('respects user overrides', () => {
        const overrides = { schema: '#FF0000', dataset: '#00FF00' };
        const result = assignAnalyticsChartColors(['schema', 'dataset'], mockTheme, overrides);

        expect(result.assignments.schema.color).toBe('#FF0000');
        expect(result.assignments.schema.userOverride).toBe('#FF0000');
        expect(result.assignments.dataset.color).toBe('#00FF00');
        expect(result.assignments.dataset.userOverride).toBe('#00FF00');
    });

    test('user overrides take precedence over entity colors', () => {
        const overrides = { dataset: '#123456' };
        const result = assignAnalyticsChartColors(['dataset'], mockTheme, overrides);

        expect(result.assignments.dataset.color).toBe('#123456');
        expect(result.assignments.dataset.color).not.toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('partial overrides work with automatic assignment', () => {
        const overrides = { entity1: '#AAAAAA' };
        const result = assignAnalyticsChartColors(['entity1', 'dataset', 'entity2'], mockTheme, overrides);

        expect(result.assignments.entity1.color).toBe('#AAAAAA');
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
        expect(result.assignments.entity2.strategy.type).toBe('qualitative');
    });
});

describe('Color Assignment Priority', () => {
    test('follows correct priority order', () => {
        const overrides = { override: '#CUSTOM' };
        const seriesKeys = ['override', 'dataset', 'unknown', 'another'];

        const result = assignAnalyticsChartColors(seriesKeys, mockTheme, overrides);

        expect(result.assignments.override.strategy.source).toBe('user-override');
        expect(result.assignments.dataset.strategy.source).toBe('datahub-entity');
        expect(result.assignments.unknown.strategy.type).toBe('qualitative');
        expect(result.assignments.another.strategy.type).toBe('qualitative');
    });

    test('does not reuse colors across different sources', () => {
        const seriesKeys = ['dataset', 'schema', 'unknown1', 'unknown2'];
        const result = assignAnalyticsChartColors(seriesKeys, mockTheme);

        const colors = Object.values(result.assignments).map((a) => a.color);
        const uniqueColors = new Set(colors);

        expect(uniqueColors.size).toBe(colors.length);
    });
});

describe('Edge Cases', () => {
    test('handles empty series array', () => {
        const result = assignAnalyticsChartColors([], mockTheme);

        expect(Object.keys(result.assignments)).toHaveLength(0);
        expect(result.generatedCount).toBe(0);
        expect(result.unusedColors.length).toBe(QUALITATIVE_COLORS.length);
    });

    test('handles single series', () => {
        const result = assignAnalyticsChartColors(['dataset'], mockTheme);

        expect(Object.keys(result.assignments)).toHaveLength(1);
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles duplicate series keys', () => {
        const result = assignAnalyticsChartColors(['dataset', 'dataset'], mockTheme);

        expect(Object.keys(result.assignments)).toHaveLength(1);
        expect(result.assignments.dataset.color).toBe(DATAHUB_ENTITY_COLORS.dataset);
    });

    test('handles very long series names', () => {
        const longName = 'a'.repeat(100);
        const result = assignAnalyticsChartColors([longName], mockTheme);

        expect(result.assignments[longName]).toBeDefined();
        expect(result.assignments[longName].color).toBeTruthy();
    });
});

describe('getAllEntityMatches utility', () => {
    test('returns all matching entities', () => {
        const matches = getAllEntityMatches('dataset', mockTheme);

        expect(matches.length).toBeGreaterThan(0);
        expect(matches.some((m) => m.entity === 'dataset')).toBe(true);
        matches.forEach((match) => {
            expect(match.color).toBeTruthy();
        });
    });

    test('returns empty array for no matches', () => {
        const matches = getAllEntityMatches('xyz123notfound', mockTheme);

        expect(matches.length).toBe(0);
    });
});
