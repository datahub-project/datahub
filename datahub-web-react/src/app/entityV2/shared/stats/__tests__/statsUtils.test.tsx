// Disabling no hardcoded colors rule in test file
/* eslint-disable rulesdir/no-hardcoded-colors */
import { DefaultTheme } from 'styled-components';

import { percentileToColor, percentileToLabel } from '@app/entityV2/shared/stats/statsUtils';

const mockTheme = {
    colors: {
        bgSurface: '#F5F5F5',
        bgSurfaceSuccess: '#F7FBF4',
        bgSurfaceSuccessHover: '#E1F0D6',
    },
} as unknown as DefaultTheme;

describe('percentileToLabel', () => {
    it('should return "Low" when percentile is <= 30', () => {
        const label = percentileToLabel(25);
        expect(label).toBe('Low');
    });

    it('should return "Med" when percentile is > 30 and <= 80', () => {
        const label = percentileToLabel(60);
        expect(label).toBe('Med');
    });

    it('should return "High" when percentile is > 80', () => {
        const label = percentileToLabel(85);
        expect(label).toBe('High');
    });
});

describe('percentileToColor', () => {
    it('should return "#F5F5F5" when percentile is <= 30', () => {
        const color = percentileToColor(25, mockTheme);
        expect(color).toBe('#F5F5F5');
    });

    it('should return "#F7FBF4" when percentile is > 30 and <= 80', () => {
        const color = percentileToColor(60, mockTheme);
        expect(color).toBe('#F7FBF4');
    });

    it('should return "#E1F0D6" when percentile is > 80', () => {
        const color = percentileToColor(85, mockTheme);
        expect(color).toBe('#E1F0D6');
    });
});
