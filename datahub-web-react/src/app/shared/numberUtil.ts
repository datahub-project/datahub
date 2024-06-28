export function parseMaybeStringAsFloatOrDefault<T>(str: any, fallback?: T): number | T | undefined {
    const parsedValue = typeof str === 'string' ? parseFloat(str) : str;
    return typeof parsedValue === 'number' && !Number.isNaN(parsedValue) ? parsedValue : fallback;
}

export function parseJsonArrayOrDefault<T>(str: any, fallback: T[] = []): T[] | undefined {
    // Check if the input is a string and try to parse it.
    if (typeof str === 'string') {
        try {
            const parsedValue = JSON.parse(str);
            // Check if the parsed value is an array before returning.
            if (Array.isArray(parsedValue)) {
                return parsedValue;
            }
        } catch (e) {
            // If parsing throws, log the error (optional) and proceed to return fallback.
            console.error('Failed to parse JSON:', e);
        }
    }
    // Return fallback if the above conditions fail.
    return fallback;
}
