export function parseMaybeStringAsFloatOrDefault<T>(str: any, fallback?: T): number | T | undefined {
    const parsedValue = typeof str === 'string' ? parseFloat(str) : str;
    return typeof parsedValue === 'number' && !Number.isNaN(parsedValue) ? parsedValue : fallback;
}
