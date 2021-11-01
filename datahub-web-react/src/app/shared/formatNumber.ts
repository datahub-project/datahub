export function formatNumber(n) {
    if (n < 1e3) return n;
    if (n >= 1e3 && n < 1e6) return `${+(n / 1e3).toFixed(1)}K`;
    if (n >= 1e6) return `${+(n / 1e6).toFixed(1)}M`;
    return '';
}
