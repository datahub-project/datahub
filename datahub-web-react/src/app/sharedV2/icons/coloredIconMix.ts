/**
 * Shared color-mix formulas for tinted entity icons (domains, glossary, tags).
 * Keep ratios in one place so pickers and rendered badges stay visually aligned.
 */
export function coloredIconForeground(hex: string, textColor: string): string {
    return `color-mix(in srgb, ${hex} 75%, ${textColor})`;
}

export function coloredIconBackground(hex: string, bgColor: string): string {
    return `color-mix(in srgb, ${hex} 12%, ${bgColor})`;
}
