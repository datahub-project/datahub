import { useCustomTheme } from '@src/customThemeContext';

export type ColorPalette = { [key: string]: string };

export const GlossaryV2PaletteColors: ColorPalette = {
    PASTEL_LAVENDER: '#9386E2',
    PURPLE: '#9254DE',
    LIGHT_BLUE: '#85A5FF',
    BLUE: '#378BF2',
    FRESH_TEAL: '#40B0A9',
    GREEN: '#5EAA32',
    GREENISH_LIME: '#9BB832',
    LIGHT_ORANGE: '#D8A42C',
    PASTEL_MUSTARD: '#E1AF63',
    ORANGE: '#E5993E',
    PEACH_ORANGE: '#E58356',
    RED: '#C06F6F',
    PASTEL_MAGENTA: '#D885AD',
    COLD_GREY: '#81879F',
};

export const getStringHash = (str: string) => {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        /* eslint-disable no-bitwise */
        hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    return hash;
};

export function useGenerateGlossaryColorFromPalette() {
    const { theme } = useCustomTheme();

    const generateColor = (urn: string) => {
        const palette = theme?.colors?.glossaryPalette || Object.values(GlossaryV2PaletteColors);
        const colorIndex = Math.abs(getStringHash(urn)) % palette.length;
        return palette[colorIndex];
    };

    return generateColor;
}
