/**
 * Analytics Chart Color Assignment System
 *
 * Implements a smart, priority-based color assignment strategy:
 * 1. User overrides (manual customization)
 * 2. Semantic mapping (DataHub entity types)
 * 3. Qualitative palette (high-contrast colors)
 * 4. Generated colors (unlimited series via HSL)
 */
import { QUALITATIVE_COLORS, generateDistinctColors } from '@app/analyticsDashboardV2/utils/chartColorConstants';
import { findDataHubEntityColor } from '@app/analyticsDashboardV2/utils/chartColorMatcher';

/**
 * Describes the strategy used to assign a color
 */
export interface ColorStrategy {
    type: 'semantic' | 'qualitative' | 'generated';
    source: string;
}

/**
 * Complete configuration for a series color
 */
export interface SeriesColorConfig {
    key: string;
    label: string;
    color: string;
    strategy: ColorStrategy;
    userOverride?: string;
}

/**
 * Result of color assignment operation
 */
export interface ColorAssignmentResult {
    assignments: Record<string, SeriesColorConfig>;
    unusedColors: string[];
    generatedCount: number;
}

/**
 * Assign colors to analytics chart series using intelligent priority system
 *
 * Priority order:
 * 1. Existing user overrides
 * 2. DataHub entity semantic colors
 * 3. High-contrast qualitative palette
 * 4. Dynamically generated colors
 *
 * @param seriesKeys - Array of series identifiers
 * @param existingOverrides - Manual color overrides from user
 * @returns Complete color assignment result
 */
export function assignAnalyticsChartColors(
    seriesKeys: string[],
    existingOverrides: Record<string, string> = {},
): ColorAssignmentResult {
    const assignments: Record<string, SeriesColorConfig> = {};
    const usedColors = new Set<string>();
    let generatedCount = 0;

    // Step 1: Apply any existing overrides first
    seriesKeys.forEach((key) => {
        if (existingOverrides[key]) {
            assignments[key] = {
                key,
                label: key,
                color: existingOverrides[key],
                strategy: { type: 'semantic', source: 'user-override' },
                userOverride: existingOverrides[key],
            };
            usedColors.add(existingOverrides[key]);
        }
    });

    // Step 2: DataHub entity semantic matching
    const remainingKeys = seriesKeys.filter((key) => !assignments[key]);

    remainingKeys.forEach((key) => {
        const entityColor = findDataHubEntityColor(key);
        if (entityColor && !usedColors.has(entityColor)) {
            assignments[key] = {
                key,
                label: key,
                color: entityColor,
                strategy: { type: 'semantic', source: 'datahub-entity' },
            };
            usedColors.add(entityColor);
        }
    });

    // Step 3: High-contrast qualitative colors
    const stillRemaining = seriesKeys.filter((key) => !assignments[key]);
    const availableQualitative = QUALITATIVE_COLORS.filter((color) => !usedColors.has(color));

    stillRemaining.slice(0, availableQualitative.length).forEach((key, index) => {
        assignments[key] = {
            key,
            label: key,
            color: availableQualitative[index],
            strategy: { type: 'qualitative', source: 'colorbrewer-set3' },
        };
        usedColors.add(availableQualitative[index]);
    });

    // Step 4: Generated colors for any remaining
    const finalRemaining = seriesKeys.filter((key) => !assignments[key]);

    if (finalRemaining.length > 0) {
        const generatedColors = generateDistinctColors({
            count: finalRemaining.length,
            avoidColors: Array.from(usedColors),
        });

        finalRemaining.forEach((key, index) => {
            assignments[key] = {
                key,
                label: key,
                color: generatedColors[index],
                strategy: { type: 'generated', source: 'hsl-golden-ratio' },
            };
        });

        generatedCount = finalRemaining.length;
    }

    return {
        assignments,
        unusedColors: QUALITATIVE_COLORS.filter((c) => !usedColors.has(c)),
        generatedCount,
    };
}

/**
 * Get statistics about color assignment strategies used
 */
export function getColorAssignmentStats(result: ColorAssignmentResult) {
    const assignments = Object.values(result.assignments);

    return {
        total: assignments.length,
        entityMatched: assignments.filter((a) => a.strategy.source === 'datahub-entity').length,
        qualitative: assignments.filter((a) => a.strategy.type === 'qualitative').length,
        generated: result.generatedCount,
        userOverrides: assignments.filter((a) => a.userOverride).length,
    };
}
