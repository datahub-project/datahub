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
