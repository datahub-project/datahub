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

export const DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL = 7;
