export function applyOpacity(hexColor: string, opacity: number) {
    if (hexColor.length !== 7) return hexColor;

    const updatedOpacity = Math.round(opacity * 2.55);

    return hexColor + updatedOpacity.toString(16).padStart(2, '0');
}

/**
 * TODO: merge this with {@link #applyOpacity} above
 */
export function applyOpacityToHexColor(hex, opacity) {
    // Ensure the hex color is valid and remove any leading #
    const finalHex = hex.replace(/^#/, '');

    // Convert opacity from 0-1 range to 0-255 range
    const alpha = Math.round(opacity * 255);

    // Convert the alpha value to a hex string and ensure it's 2 characters long
    const alphaHex = (alpha + 0x100).toString(16).substr(-2);

    // Return the original hex color with the alpha opacity appended
    return `#${finalHex}${alphaHex}`;
}
