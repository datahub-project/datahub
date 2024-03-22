
export function parseMaybeStringAsFloatOrDefault(str: any, fallback?: number): number | undefined {
    const parsedValue = typeof str === 'string' ? parseFloat(str) : str;
    return typeof parsedValue === 'number' && !Number.isNaN(parsedValue) ? parsedValue : fallback;
}