export const getLighterRGBColor = (red, green, blue) => {
    const lighterRed = red + 0.9 * (255 - red);
    const lighterGreen = green + 0.9 * (255 - green);
    const lighterBlue = blue + 0.9 * (255 - blue);

    return [lighterRed, lighterGreen, lighterBlue];
};
