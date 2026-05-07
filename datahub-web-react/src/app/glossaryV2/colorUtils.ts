import { DefaultTheme, useTheme } from 'styled-components';

type ColorPalette = { [key: string]: string };

const getGlossaryV2PaletteColors = (theme: DefaultTheme): ColorPalette => ({
    PASTEL_LAVENDER: theme.colors.glossaryPaletteViolet,
    PURPLE: theme.colors.glossaryPalettePurple,
    LIGHT_BLUE: theme.colors.glossaryPaletteLightBlue,
    BLUE: theme.colors.glossaryPaletteBlue,
    FRESH_TEAL: theme.colors.glossaryPaletteTeal,
    GREEN: theme.colors.glossaryPaletteGreen,
    GREENISH_LIME: theme.colors.glossaryPaletteLime,
    LIGHT_ORANGE: theme.colors.glossaryPaletteLightOrange,
    PASTEL_MUSTARD: theme.colors.glossaryPaletteMustard,
    ORANGE: theme.colors.glossaryPaletteOrange,
    PEACH_ORANGE: theme.colors.glossaryPalettePeach,
    RED: theme.colors.glossaryPaletteRed,
    PASTEL_MAGENTA: theme.colors.glossaryPaletteMagenta,
    COLD_GREY: theme.colors.glossaryPaletteColdGrey,
});

export const getStringHash = (str: string) => {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        /* eslint-disable no-bitwise */
        hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    return hash;
};

export function useGenerateGlossaryColorFromPalette() {
    const theme = useTheme();

    const generateColor = (urn: string) => {
        const palette = theme?.colors?.glossaryPalette || Object.values(getGlossaryV2PaletteColors(theme));
        const colorIndex = Math.abs(getStringHash(urn)) % palette.length;
        return palette[colorIndex];
    };

    return generateColor;
}
