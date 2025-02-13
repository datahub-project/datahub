import { colors } from '@src/alchemy-components/theme';
import { ColorScheme, ColorSchemeParams } from './types';

export const VIOLET_COLOR_SCHEME_PRARAMS: ColorSchemeParams = {
    mainColor: colors.violet[500],
    alternativeColor: '#917FFF',
};

export const BLUE_COLOR_SCHEME_PARAMS: ColorSchemeParams = {
    mainColor: colors.blue[400],
    alternativeColor: '#CCEBF6',
};

export const RED_COLOR_SCHEME_PARAMS: ColorSchemeParams = {
    mainColor: colors.red[400],
    alternativeColor: colors.red[200],
};

export const ORANGE_COLOR_SCHEME_PARAMS: ColorSchemeParams = {
    mainColor: '#FFD8B1',
    alternativeColor: '#FFF3E0',
};

export const GREEN_COLOR_SCHEME_PARAMS: ColorSchemeParams = {
    mainColor: colors.green[400],
    alternativeColor: colors.green[200],
};

export const COLOR_SCHEMES: ColorScheme[] = Object.values(ColorScheme);

export const DEFAULT_COLOR_SCHEME = ColorScheme.Violet;

export const COLOR_SCHEME_TO_PARAMS = {
    [ColorScheme.Violet]: VIOLET_COLOR_SCHEME_PRARAMS,
    [ColorScheme.Blue]: BLUE_COLOR_SCHEME_PARAMS,
    [ColorScheme.Pink]: RED_COLOR_SCHEME_PARAMS,
    [ColorScheme.Orange]: ORANGE_COLOR_SCHEME_PARAMS,
    [ColorScheme.Green]: GREEN_COLOR_SCHEME_PARAMS,
};

export const DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL = 7;
