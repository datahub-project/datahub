export function formatNumber(n) {
    if (n < 1e3) return n;
    if (n >= 1e3 && n < 1e6) return `${+(n / 1e3).toFixed(1)}k`;
    if (n >= 1e6 && n < 1e9) return `${+(n / 1e6).toFixed(1)}M`;
    if (n >= 1e9) return `${+(n / 1e9).toFixed(1)}B`;
    return '';
}

export function formatNumberWithoutAbbreviation(n) {
    return n.toLocaleString();
}

export function formatBytes(bytes: number, decimals = 2, bytesUnit = 'Bytes'): { number: number; unit: string } {
    if (!bytes)
        return {
            number: 0,
            unit: bytesUnit,
        };

    const k = 1000; // We use IEEE standards definition of units of byte, where 1000 bytes = 1kb.
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = [bytesUnit, 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return {
        // eslint-disable-next-line no-restricted-properties
        number: parseFloat((bytes / Math.pow(k, i)).toFixed(dm)),
        unit: sizes[i],
    };
}
