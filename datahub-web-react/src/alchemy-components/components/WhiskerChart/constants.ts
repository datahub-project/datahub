import { TextProps } from '@visx/text/lib/Text';
import { DefaultTheme } from 'styled-components';

import { ColorSchemeSettings, WhiskerMetricType } from '@components/components/WhiskerChart/types';

export const getDefaultColorScheme = (theme: DefaultTheme): ColorSchemeSettings => ({
    box: theme.colors.chartsBrandMedium,
    boxAlternative: theme.colors.chartsBrandBase,
    medianLine: theme.colors.chartsBrandContrast,
    alternative: theme.colors.chartsBrandHigh,
});

export const WHISKER_METRIC_NAMES = {
    [WhiskerMetricType.Max]: 'Max',
    [WhiskerMetricType.FirstQuartile]: 'First Quartile',
    [WhiskerMetricType.Median]: 'Median',
    [WhiskerMetricType.ThirdQuartile]: 'Third Quartile',
    [WhiskerMetricType.Min]: 'Min',
};

export const WHISKER_METRIC_ATTRIBUTE_NAMES = {
    [WhiskerMetricType.Max]: 'max',
    [WhiskerMetricType.FirstQuartile]: 'firstQuartile',
    [WhiskerMetricType.Median]: 'median',
    [WhiskerMetricType.ThirdQuartile]: 'thirdQuartile',
    [WhiskerMetricType.Min]: 'min',
};

export const DEFAULT_BOX_SIZE = 34;
export const DEFAULT_GAP_BETWEEN_WHISKERS = 10;

export const AXIS_LABEL_PROPS: Partial<TextProps> = {
    fontSize: 12,
    fontFamily: 'Mulish',
    fontWeight: 600,
    textAnchor: 'middle',
};

export const AXIS_LABEL_MARGIN_OFFSET = 30;
