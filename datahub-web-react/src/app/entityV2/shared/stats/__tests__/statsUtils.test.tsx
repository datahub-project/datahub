import { ANTD_GRAY } from '../../constants';
import { percentileToColor, percentileToLabel } from '../statsUtils';

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
    it('should return ANTD_GRAY[3] when percentile is <= 30', () => {
        const color = percentileToColor(25);
        expect(color).toBe(ANTD_GRAY[3]);
    });

    it('should return "#EBF3F2" when percentile is > 30 and <= 80', () => {
        const color = percentileToColor(60);
        expect(color).toBe('#EBF3F2');
    });

    it('should return "#cef5f0" when percentile is > 80', () => {
        const color = percentileToColor(85);
        expect(color).toBe('#cef5f0');
    });
});
