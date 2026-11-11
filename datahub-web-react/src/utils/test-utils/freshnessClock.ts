import { screen } from '@testing-library/react';

import { getLastIngestedColor } from '@app/entityV2/shared/containers/profile/sidebar/LastIngested';
import { hexToRgb } from '@app/sharedV2/colors/colorUtils';
import lightTheme from '@conf/theme/colorThemes/light';

const toRgb = (hex: string) => `rgb(${hexToRgb(hex).join(', ')})`;

export const SIXTY_DAYS_AGO = Date.now() - 60 * 24 * 60 * 60 * 1000;
export const AGE_COLOR = toRgb(getLastIngestedColor(SIXTY_DAYS_AGO));
export const NEUTRAL = toRgb(lightTheme.icon);

export const clockColor = () =>
    getComputedStyle(screen.getByTestId('freshness').querySelector('svg') as SVGElement).color;
