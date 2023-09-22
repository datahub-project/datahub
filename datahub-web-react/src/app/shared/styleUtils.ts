export function applyOpacity(hexColor: string, opacity: number) {
    if (hexColor.length !== 7) return hexColor;

    const updatedOpacity = Math.round(opacity * 2.55);

    return hexColor + updatedOpacity.toString(16).padStart(2, '0');
}
