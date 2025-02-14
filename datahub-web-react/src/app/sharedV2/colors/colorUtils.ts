export function hexToRgba(rawHex: string, opacity: number): string {
    const [r, g, b] = hexToRgb(rawHex);

    // Ensure opacity is a value between 0 and 1
    const resultOpacity = Math.min(1, Math.max(0, opacity));

    // Create the RGBA color string
    const rgba = `rgba(${r}, ${g}, ${b}, ${resultOpacity})`;

    return rgba;
}

export function hexToRgb(rawHex: string): [number, number, number] {
    let hex = rawHex.replace('#', '');
    if (hex.length === 3) {
        hex += hex;
    }
    return [parseInt(hex.substring(0, 2), 16), parseInt(hex.substring(2, 4), 16), parseInt(hex.substring(4, 6), 16)];
}

/**
 * Create an opaque color, that has the same hue as the input color with the specified opacity against the background color.
 * @param hex The input color in hex format.
 * @param backgroundHex The background color in hex format.
 * @param opacity The opacity value between 0 and 100.
 */
export function applyOpacity(hex: string, backgroundHex: string, opacity: number) {
    const [inputR, inputG, inputB] = hexToRgb(hex);
    const [backgroundR, backgroundG, backgroundB] = hexToRgb(backgroundHex);
    const r = Math.round((1 - opacity / 100) * backgroundR + (opacity / 100) * inputR);
    const g = Math.round((1 - opacity / 100) * backgroundG + (opacity / 100) * inputG);
    const b = Math.round((1 - opacity / 100) * backgroundB + (opacity / 100) * inputB);
    return `rgb(${r}, ${g}, ${b})`;
}
