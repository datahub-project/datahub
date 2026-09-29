import { TextProps } from '@visx/text/lib/Text';
import { TFunction } from 'i18next';
import { DefaultTheme } from 'styled-components';

import { ColorSchemeSettings, WhiskerMetricType } from '@components/components/WhiskerChart/types';

export const getDefaultColorScheme = (theme: DefaultTheme): ColorSchemeSettings => ({
    box: theme.colors.chartsBrandMedium,
    boxAlternative: theme.colors.chartsBrandBase,
    medianLine: theme.colors.chartsBrandContrast,
    alternative: theme.colors.chartsBrandHigh,
});

export const getWhiskerMetricNames = (t: TFunction): Record<WhiskerMetricType, string> => ({
    [WhiskerMetricType.Max]: t('whiskerChart.metric.max'),
    [WhiskerMetricType.FirstQuartile]: t('whiskerChart.metric.firstQuartile'),
    [WhiskerMetricType.Median]: t('whiskerChart.metric.median'),
    [WhiskerMetricType.ThirdQuartile]: t('whiskerChart.metric.thirdQuartile'),
    [WhiskerMetricType.Min]: t('whiskerChart.metric.min'),
});

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
