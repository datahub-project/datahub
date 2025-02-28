import { TextProps } from '@visx/text/lib/Text';
import { ColorSchemeSettings, WhiskerMetricType } from './types';

export const DEFAULT_COLOR_SHEME: ColorSchemeSettings = {
    box: '#705EE4',
    boxAlternative: '#CAC3F1',
    medianLine: '#2200F9',
    alternative: '#533FD1',
};

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
