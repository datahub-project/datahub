import { hexToRgb, hexToRgba } from '@app/sharedV2/colors/colorUtils';

describe('hexToRgba', () => {
    it('should convert hex to rgba with opacity 1', () => {
        const hex = '#FF5733';
        const opacity = 1;
        const rgba = hexToRgba(hex, opacity);
        expect(rgba).toBe('rgba(255, 87, 51, 1)');
    });

    it('should convert hex to rgba with opacity 0.5', () => {
        const hex = '#33FF57';
        const opacity = 0.5;
        const rgba = hexToRgba(hex, opacity);
        expect(rgba).toBe('rgba(51, 255, 87, 0.5)');
    });

    it('should clamp opacity between 0 and 1', () => {
        const hex = '#000000';
        const opacity = -1;
        const rgba = hexToRgba(hex, opacity);
        expect(rgba).toBe('rgba(0, 0, 0, 0)');
    });
});

describe('hexToRgb', () => {
    it('should convert hex to rgb', () => {
        const hex = '#FF5733';
        const rgb = hexToRgb(hex);
        expect(rgb).toEqual([255, 87, 51]);
    });

    it('should handle lowercase hex', () => {
        const hex = '#ff5733';
        const rgb = hexToRgb(hex);
        expect(rgb).toEqual([255, 87, 51]);
    });
});
