import { ColorScheme, ColorSchemeParams } from '@components/components/BarChart/types';

import ColorTheme from '@conf/theme/colorThemes/types';

export const getColorSchemeParams = (themeColors: ColorTheme): Record<ColorScheme, ColorSchemeParams> => ({
    [ColorScheme.Violet]: {
        mainColor: themeColors.chartsBrandMedium,
        alternativeColor: themeColors.chartsBrandMediumAlpha,
    },
    [ColorScheme.Blue]: {
        mainColor: themeColors.chartsInformationMedium,
        alternativeColor: themeColors.chartsInformationBase,
    },
    [ColorScheme.Pink]: {
        mainColor: themeColors.chartsWineMedium,
        alternativeColor: themeColors.chartsWineLow,
    },
    [ColorScheme.Orange]: {
        mainColor: themeColors.chartsTangerineMedium,
        alternativeColor: themeColors.chartsTangerineBase,
    },
    [ColorScheme.Green]: {
        mainColor: themeColors.chartsGreenMedium,
        alternativeColor: themeColors.chartsGreenLow,
    },
});

export const COLOR_SCHEMES: ColorScheme[] = Object.values(ColorScheme);

export const DEFAULT_COLOR_SCHEME = ColorScheme.Violet;

export const COLOR_SCHEME_TO_PARAMS: Record<ColorScheme, ColorSchemeParams> = {
    [ColorScheme.Violet]: { mainColor: '#705EE4', alternativeColor: '#917FFF99' },
    [ColorScheme.Blue]: { mainColor: '#4897B4', alternativeColor: '#CCEBF6' },
    [ColorScheme.Pink]: { mainColor: '#E99393', alternativeColor: '#F2C1C1' },
    [ColorScheme.Orange]: { mainColor: '#FFD8B1', alternativeColor: '#FFF3E0' },
    [ColorScheme.Green]: { mainColor: '#92C573', alternativeColor: '#C0DEAF' },
};

export const DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL = 7;
