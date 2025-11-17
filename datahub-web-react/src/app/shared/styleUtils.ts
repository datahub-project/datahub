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

/**
 * Shared navigation and list item state styles
 * Used in nav bar menu items, conversation lists, and other interactive list items
 */
export const NAV_ITEM_HOVER_GRADIENT =
    'linear-gradient(180deg, rgba(243, 244, 246, 0.5) -3.99%, rgba(235, 236, 240, 0.5) 53.04%, rgba(235, 236, 240, 0.5) 100%)';
export const NAV_ITEM_SELECTED_GRADIENT =
    'linear-gradient(180deg, rgba(83, 63, 209, 0.04) -3.99%, rgba(112, 94, 228, 0.04) 53.04%, rgba(112, 94, 228, 0.04) 100%)';
export const NAV_ITEM_HOVER_BOX_SHADOW = '0px 0px 0px 1px rgba(139, 135, 157, 0.08)';
export const NAV_ITEM_SELECTED_BOX_SHADOW = '0px 0px 0px 1px rgba(108, 71, 255, 0.08)';
