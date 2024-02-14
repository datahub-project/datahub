export function hexToRgba(rawHex: string, opacity: number): string {
    const [r, g, b] = hexToRgb(rawHex);

    // Ensure opacity is a value between 0 and 1
    const resultOpacity = Math.min(1, Math.max(0, opacity));

    // Create the RGBA color string
    const rgba = `rgba(${r}, ${g}, ${b}, ${resultOpacity})`;

    return rgba;
}

export function hexToRgb(rawHex: string): [number, number, number] {
    const hex = rawHex.replace('#', '');
    return [parseInt(hex.substring(0, 2), 16), parseInt(hex.substring(2, 4), 16), parseInt(hex.substring(4, 6), 16)];
}
