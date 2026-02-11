/**
 * Color constants and palettes for DataHub Analytics Charts
 *
 * This file contains:
 * - Semantic color mappings for DataHub entity types
 * - High-contrast qualitative color palettes
 * - Colorblind-safe fallback palettes
 */

/**
 * Semantic color dictionary for DataHub entity types
 * Colors are organized by category for better visual grouping
 */
export const DATAHUB_ENTITY_COLORS: Record<string, string> = {
    // Core Data Assets (Blues)
    dataset: '#1E40AF',
    dataget: '#2563EB',
    dashboard: '#3B82F6',
    chart: '#60A5FA',

    // Schema & Structure (Purples)
    schema: '#7C3AED',
    dataproduct: '#8B5CF6',
    dataplatform: '#A78BFA',

    // Quality & Validation (Oranges/Reds)
    incidents: '#DC2626',
    validation: '#EA580C',
    assertion: '#F59E0B',
    properties: '#D97706',

    // Operations & Processes (Greens)
    container: '#059669',
    glossary: '#10B981',
    corpgroup: '#16A34A',
    dataprocess: '#22C55E',

    // Platform & Infrastructure (Teals/Grays)
    dataflow: '#0891B2',
    mlmodel: '#0D9488',
    mlfeature: '#14B8A6',
    domain: '#6B7280',
};

/**
 * Default color for unknown entities
 */
export const DEFAULT_ENTITY_COLOR = '#9CA3AF';

/**
 * High-contrast qualitative palette based on ColorBrewer Set3
 * Provides reliable visual distinction for up to 12 series
 */
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

/**
 * Okabe-Ito colorblind-safe palette
 * Backup palette optimized for deuteranopia/protanopia
 */
export const COLORBLIND_SAFE_COLORS = [
    '#E69F00',
    '#56B4E9',
    '#009E73',
    '#F0E442',
    '#0072B2',
    '#D55E00',
    '#CC79A7',
    '#999999',
];

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
