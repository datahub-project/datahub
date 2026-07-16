/**
 * Color constants and palettes for DataHub Analytics Charts
 *
 * This file contains:
 * - Semantic color mappings for DataHub entity types
 * - High-contrast qualitative color palettes
 */
import { DefaultTheme } from 'styled-components';

/**
 * Semantic color dictionary for DataHub entity types
 * Colors are organized by category for better visual grouping
 */
export const getDatahubEntityColors = (theme: DefaultTheme) => ({
    // Core Data Assets (Blues)
    dataset: theme.colors.tagsCobaltBlueText,
    dataget: theme.colors.chartsInformationHigh,
    dashboard: theme.colors.chartsInformationMedium,
    chart: theme.colors.chartsInformationLow,

    // Schema & Structure (Purples)
    schema: theme.colors.chartsBrandHigh,
    dataproduct: theme.colors.chartsBrandMedium,
    dataplatform: theme.colors.chartsBrandLow,

    // Quality & Validation (Oranges/Reds)
    incidents: theme.colors.iconError,
    validation: theme.colors.chartsRedMedium,
    assertion: theme.colors.iconWarning,
    properties: theme.colors.textWarning,

    // Operations & Processes (Greens)
    container: theme.colors.iconSuccess,
    glossary: theme.colors.chartsGreenHigh,
    corpgroup: theme.colors.chartsGreenMedium,
    dataprocess: theme.colors.chartsGreenLow,

    // Platform & Infrastructure (Teals/Grays)
    dataflow: theme.colors.chartsBlueHigh,
    mlmodel: theme.colors.chartsSeafoamHigh,
    mlfeature: theme.colors.chartsSeafoamMedium,
    domain: theme.colors.textTertiary,
});

/**
 * High-contrast qualitative palette based on ColorBrewer Set3
 * Provides reliable visual distinction for up to 12 series
 */

// Disabling no hardcoded colors rule for qualitative colors because it's and external ColorBrewer Set3
/* eslint-disable rulesdir/no-hardcoded-colors */
export const QUALITATIVE_COLORS = [
    '#8dd3c7',
    '#ffffb3',
    '#bebada',
    '#fb8072',
    '#80b1d3',
    '#fdb462',
    '#b3de69',
    '#fccde5',
    '#d9d9d9',
    '#bc80bd',
    '#ccebc5',
    '#ffed6f',
];
/* eslint-enable rulesdir/no-hardcoded-colors */

/**
 * Options for dynamic color generation
 */
export interface ColorGenerationOptions {
    count: number;
    startHue?: number;
    saturationRange?: [number, number];
    lightnessRange?: [number, number];
    avoidColors?: string[];
}

/**
 * Generate distinct colors using HSL and golden ratio distribution
 * Ensures visual distinction for unlimited series beyond predefined palettes
 */
export function generateDistinctColors(options: ColorGenerationOptions): string[] {
    const { count, startHue = 0, saturationRange = [60, 90], lightnessRange = [40, 70] } = options;

    const colors: string[] = [];
    const goldenRatio = 0.618033988749;

    for (let i = 0; i < count; i++) {
        const hue = (startHue + i * goldenRatio * 360) % 360;
        const saturation = saturationRange[0] + ((i % 3) * (saturationRange[1] - saturationRange[0])) / 2;
        const lightness = lightnessRange[0] + ((i % 2) * (lightnessRange[1] - lightnessRange[0])) / 1;

        colors.push(`hsl(${hue}, ${saturation}%, ${lightness}%)`);
    }

    return colors;
}
